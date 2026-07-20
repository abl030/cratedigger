"""FakePipelineDB — the in-memory PipelineDB stand-in.

Records state transitions, log rows, denylist entries, and cooldowns
in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.
"""

from __future__ import annotations

import copy
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Optional, Sequence, cast
import msgspec


if TYPE_CHECKING:
    from cratedigger import TrackRecord
    from lib.quality import CandidateScore
    from lib.pipeline_db import SaturationSummary, SearchLogHistoryPage

from lib.import_queue import (
    ImportJob,
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WAITING,
    validate_job_type,
    validate_preview_failure_status,
    validate_payload,
    validate_status,
)
from lib.pipeline_db import (ActiveSearchPlan, BACKOFF_BASE_MINUTES,
                             DOWNLOAD_LOG_OUTCOMES,
                             JELLYFIN_PIN_STATUSES,
                             JELLYFIN_TERMINAL_PIN_STATUSES,
                             JellyfinTerminalPinStatus,
                             PLEX_PIN_STATUSES,
                             PLEX_TERMINAL_PIN_STATUSES,
                             PlexTerminalPinStatus,
                             BACKOFF_MAX_MINUTES, BadAudioHashInput,
                             BadAudioHashRow, ConsumedAttemptInput,
                             ConsumedAttemptResult, CURSOR_UPDATE_ADVANCED,
                             CURSOR_UPDATE_STALE, CURSOR_UPDATE_UNCHANGED,
                             CURSOR_UPDATE_WRAPPED, DownloadLogCounts,
                             DryRunPlanClassification,
                             NonConsumingAttemptInput,
                             PLAN_STATUS_ACTIVE, PLAN_STATUS_FAILED_DETERMINISTIC,
                             PLAN_STATUS_FAILED_TRANSIENT,
                             PLAN_STATUS_SUPERSEDED,
                             PersistedYoutubeRow,
                             RequestSpectralStateUpdate,
                             ReplacedRequestMutationError,
                             SEARCH_LOG_STAGE_ACCEPTED,
                             SEARCH_LOG_STAGE_PRE_ATTEMPT,
                             SEARCH_LOG_STAGE_STALE_COMPLETION,
                             SearchPlanInspection, SearchPlanItemInput,
                             SearchPlanItemProvenance, SearchPlanItemRow,
                             SearchPlanMetadataSnapshot,
                             SearchPlanProvenance, SearchPlanRow,
                             TransferLedgerRow,
                             WantedReconciliationCandidate)
from lib.pipeline_db._shared import (
    validate_request_metadata_fields,
)
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    CooldownConfig,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    LOSSLESS_CODECS,
    V0_PROBE_LOSSLESS_SOURCE,
    V0_PROBE_NATIVE_LOSSY_RESEARCH,
)
from lib import transitions
from lib.terminal_outcomes import (
    ImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalOutcomeResult,
    operator_search_stop_is_current,
)
from lib.beets_db import ReleaseLocation
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    normalize_release_id,
)
from lib.search_scheduler import (
    NEW_REQUEST_PRIORITY_HOURS,
    search_cohort_slots,
)


class _FakeTerminalTransitionsDB:
    """Emit production-shaped write boundaries while mutating the fake."""

    def __init__(
        self,
        db: "FakePipelineDB",
        boundary: Callable[[str], None],
    ) -> None:
        self._db = db
        self._boundary = boundary

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        return self._db.get_request(request_id)

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        del request_id, state_json, expected_status
        raise ValueError("terminal outcomes cannot transition to downloading")

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        row = self.get_request(request_id)
        return bool(row is not None and row["status"] == expected_status)

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        applied = self._db.reset_to_wanted(
            request_id,
            expected_status=expected_status,
            clear_retry_counters=clear_retry_counters,
            **fields,
        )
        if applied:
            self._boundary("request.wanted")
        return applied

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        applied = self._db.reset_downloading_to_wanted(
            request_id,
            expected_status=expected_status,
            **fields,
        )
        if applied:
            self._boundary("request.wanted")
        return applied

    def apply_wanted_policy_without_requeue(
        self,
        request_id: int,
        *,
        expected_status: str,
        fields: dict[str, object],
        attempt_type: str | None,
    ) -> bool:
        row = self._db._requests.get(request_id)
        if row is None or row.get("status") != expected_status:
            return False
        updates = dict(fields)
        if "min_bitrate" in updates and "prev_min_bitrate" not in updates:
            current_min_bitrate = row.get("min_bitrate")
            updates["prev_min_bitrate"] = (
                current_min_bitrate
                if current_min_bitrate is not None
                else row.get("prev_min_bitrate")
            )
        if updates:
            applied = self._db.update_request_fields(
                request_id,
                expected_status=expected_status,
                **updates,
            )
            if not applied:
                return False
            self._boundary("request.wanted_policy")
        if attempt_type is not None:
            return self.record_attempt(
                request_id,
                attempt_type,
                expected_status=expected_status,
            )
        return True

    def apply_terminal_metadata_without_transition(
        self,
        request_id: int,
        *,
        expected_status: str,
        fields: dict[str, object],
    ) -> bool:
        row = self._db._requests.get(request_id)
        if row is None or row.get("status") != expected_status:
            return False
        if not fields:
            return True
        applied = self._db.update_request_fields(
            request_id,
            expected_status=expected_status,
            **fields,
        )
        if applied:
            self._boundary("request.metadata")
        return applied

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        applied = self._db.record_attempt(
            request_id,
            attempt_type,
            expected_status=expected_status,
        )
        if applied:
            self._boundary(f"request.attempt.{attempt_type}")
        return applied

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        applied = self._db.mark_imported_with_rescue(
            request_id,
            expected_status=expected_status,
            **extra,
        )
        if applied:
            self._boundary("request.imported")
            if extra:
                self._boundary("request.metadata")
        return applied

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        applied = self._db.update_status(
            request_id,
            status,
            expected_status=expected_status,
            **extra,
        )
        if applied:
            self._boundary("request.status")
            if extra:
                self._boundary("request.metadata")
        return applied
from lib.validation_envelope import (
    VALIDATION_PROJECTION_UNSET,
    ValidationProjectionUnset,
    WrongMatchTriageAudit,
    derive_validation_log_columns,
)
from lib.search_classification import (
    SearchSummary as _SearchSummary,
    classify_failure_class as _classify_failure_class,
)


from tests.fakes._shared import _EPOCH, _PERTH_TZ, _as_datetime, _utcnow
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

@dataclass
class _FakeSearchPlanRow:
    """In-memory mirror of a search_plans row."""
    id: int
    request_id: int
    generator_id: str
    status: str
    failure_class: str | None = None
    metadata_snapshot: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None
    error_message: str | None = None
    superseded_at: datetime | None = None
    superseded_by_plan_id: int | None = None
    created_at: datetime = field(default_factory=_utcnow)


@dataclass
class _FakeSearchPlanItemRow:
    """In-memory mirror of a search_plan_items row."""
    id: int
    plan_id: int
    ordinal: int
    strategy: str
    query: str
    canonical_query_key: str | None = None
    repeat_group: str | None = None
    provenance: dict[str, Any] | None = None




class FakePipelineDB:
    """In-memory fake for PipelineDB — records mutations for test assertions.

    Stores request rows in a dict keyed by request_id. Mutations update the
    row in place so tests can inspect final state.

    Usage:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        # ... run orchestration code with db ...
        assert db.request(42)["status"] == "imported"
        assert len(db.download_logs) == 1
        assert db.download_logs[0].outcome == "success"
    """

    def __init__(self) -> None:
        self._requests: dict[int, dict[str, Any]] = {}
        self._tracks: dict[int, list[dict[str, Any]]] = {}
        self.download_logs: list[DownloadLogRow] = []
        self._import_jobs: list[dict[str, Any]] = []
        self.search_logs: list[SearchLogRow] = []
        self.cycle_metrics: list[dict[str, Any]] = []
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
        self.denylist: list[DenylistEntry] = []
        self.persist_import_terminal_outcome_calls: list[ImportTerminalOutcome] = []
        self.persist_preview_terminal_outcome_calls: list[PreviewTerminalOutcome] = []
        self.bad_audio_hashes: list[BadAudioHashRow] = []
        # Call-count tracking for the bad-audio-hash gate. Tests that
        # used to assert ``mock.assert_called_once()`` / ``assert_not_called()``
        # on the MagicMock-source can now inspect these instead.
        self.has_any_bad_audio_hashes_calls: int = 0
        self.lookup_bad_audio_hash_calls: list[tuple[bytes, str]] = []
        # ``clear_on_disk_quality_fields`` is invoked when a release is
        # purged from beets; tests on lib.release_cleanup assert the
        # exact request_id flushed (and whether it fired at all).
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
        self.update_download_state_current_path_calls: list[tuple[int, str | None]] = []
        self.mark_import_subprocess_started_calls: list[tuple[int, str]] = []
        self.advisory_lock_calls: list[tuple[int, int]] = []
        self.closed = False
        self._next_request_id = 0
        self._next_download_log_id = 0
        self._next_import_job_id = 0
        self._next_search_log_id = 0
        self._cooldown_result: bool | Callable[[str], bool] = False
        self._advisory_lock_result: (
            bool | Callable[[int, int], bool]) = True
        # Per-request failure injection for the active_download_state
        # writers (issue #564 review): ``set_update_download_state_error``
        # makes ``update_download_state`` (and, via delegation, its
        # status-guarded ``_if_downloading`` variant) raise for one
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
        # Migration 034 — youtube_album_mappings. Keyed by
        # (release_group_identifier, source); each value is the full
        # matrix the resolver scored for that pair. Refresh always
        # replaces the whole list (no partial updates per R14).
        self._youtube_album_mappings: dict[
            tuple[str, str], list[dict[str, Any]],
        ] = {}
        self._next_youtube_mapping_id = 0

    # --- Seeding ---

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

    def seed_request(self, row: dict[str, Any]) -> None:
        """Add a request row to the fake DB. Must include 'id'.

        Re-seeding an existing id replaces that row (an update); a NEW id
        carrying a non-NULL ``mb_release_id`` already held by another row
        raises ``UniqueViolation``, mirroring the production schema.
        """
        rid = row["id"]
        self._assert_mb_release_id_unique(
            row.get("mb_release_id"), exclude_id=rid)
        self._requests[rid] = copy.deepcopy(row)
        if rid > self._next_request_id:
            self._next_request_id = rid

    def request(self, request_id: int) -> dict[str, Any]:
        """Get a request row (for test assertions). Raises KeyError if missing."""
        return self._requests[request_id]

    # --- Migration 040: Plex addedAt pin store ---

    def add_plex_added_at_pin(
        self,
        *,
        imported_path: str,
        original_added_at: int,
        rating_key: str | None,
        request_id: int | None,
    ) -> int:
        self._next_plex_pin_id += 1
        pin_id = self._next_plex_pin_id
        self.plex_added_at_pins.append({
            "id": pin_id,
            "request_id": request_id,
            "imported_path": imported_path,
            "original_added_at": int(original_added_at),
            "rating_key": rating_key,
            "status": "pending",
            "captured_at": _utcnow(),
            "reconciled_at": None,
        })
        return pin_id

    def get_pending_plex_added_at_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(p) for p in self.plex_added_at_pins
            if p["status"] == "pending"
            and _as_datetime(p["captured_at"]) < captured_before
        ]
        rows.sort(key=lambda p: (_as_datetime(p["captured_at"]), p["id"]))
        return rows[:limit]

    def mark_plex_added_at_pin(
        self,
        pin_id: int,
        *,
        status: PlexTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        for p in self.plex_added_at_pins:
            if p["id"] == pin_id:
                if status not in PLEX_PIN_STATUSES:
                    import psycopg2.errors

                    raise psycopg2.errors.CheckViolation(
                        "new row for relation \"plex_added_at_pins\" violates "
                        "check constraint \"plex_added_at_pins_status_check\""
                    )
                p["status"] = status
                p["reconciled_at"] = reconciled_at
                return

    def prune_terminal_plex_added_at_pins(
        self,
        *,
        older_than: datetime,
    ) -> int:
        survivors = [
            p for p in self.plex_added_at_pins
            if not (
                p["status"] in PLEX_TERMINAL_PIN_STATUSES
                and p["reconciled_at"] is not None
                and _as_datetime(p["reconciled_at"]) < older_than
            )
        ]
        removed = len(self.plex_added_at_pins) - len(survivors)
        self.plex_added_at_pins[:] = survivors
        return removed

    # --- Migration 046: Jellyfin DateCreated pin store ---

    def add_jellyfin_date_created_pin(
        self,
        *,
        imported_path: str,
        original_date_created: str,
        album_item_id: str | None,
        children_item_ids: list[str],
        request_id: int | None,
    ) -> int:
        self._next_jellyfin_pin_id += 1
        pin_id = self._next_jellyfin_pin_id
        self.jellyfin_date_created_pins.append({
            "id": pin_id,
            "request_id": request_id,
            "imported_path": imported_path,
            "original_date_created": original_date_created,
            "album_item_id": album_item_id,
            "children_item_ids": list(children_item_ids),
            "status": "pending",
            "captured_at": _utcnow(),
            "reconciled_at": None,
        })
        return pin_id

    def get_pending_jellyfin_date_created_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(p) for p in self.jellyfin_date_created_pins
            if p["status"] == "pending"
            and _as_datetime(p["captured_at"]) < captured_before
        ]
        rows.sort(key=lambda p: (_as_datetime(p["captured_at"]), p["id"]))
        return rows[:limit]

    def mark_jellyfin_date_created_pin(
        self,
        pin_id: int,
        *,
        status: JellyfinTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        for p in self.jellyfin_date_created_pins:
            if p["id"] == pin_id:
                if status not in JELLYFIN_PIN_STATUSES:
                    import psycopg2.errors

                    raise psycopg2.errors.CheckViolation(
                        "new row for relation \"jellyfin_date_created_pins\" "
                        "violates check constraint "
                        "\"jellyfin_date_created_pins_status_check\""
                    )
                p["status"] = status
                p["reconciled_at"] = reconciled_at
                return

    def prune_terminal_jellyfin_date_created_pins(
        self,
        *,
        older_than: datetime,
    ) -> int:
        survivors = [
            p for p in self.jellyfin_date_created_pins
            if not (
                p["status"] in JELLYFIN_TERMINAL_PIN_STATUSES
                and p["reconciled_at"] is not None
                and _as_datetime(p["reconciled_at"]) < older_than
            )
        ]
        removed = len(self.jellyfin_date_created_pins) - len(survivors)
        self.jellyfin_date_created_pins[:] = survivors
        return removed

    def queue_execute_results(self, *results: Any) -> None:
        """Register a deterministic cursor sequence for ``_execute`` calls.

        Each subsequent ``_execute(sql, params)`` call pops the next entry:
        - If the entry is an ``Exception`` instance/subclass, it is raised
          (so tests can simulate ``psycopg2.ProgrammingError`` etc.).
        - Otherwise the entry is returned as the cursor.

        Replaces ``MagicMock(); db._execute.side_effect = [c1, c2, c3]``.
        Inspect call args via ``db.execute_calls``.
        """
        self._execute_queue = list(results)

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Stand-in for ``PipelineDB._execute``.

        Records the call and returns the next entry from
        ``queue_execute_results``. If the queue is empty, returns
        ``self._execute_default`` (an empty :class:`FakeCursor` —
        mirrors production's "query ran, zero rows" contract)."""
        self.execute_calls.append((sql, params))
        if not self._execute_queue:
            return self._execute_default
        entry = self._execute_queue.pop(0)
        if isinstance(entry, Exception):
            raise entry
        return entry

    def set_cooldown_result(self, result: bool | Callable[[str], bool]) -> None:
        """Configure what check_and_apply_cooldown returns.

        Pass a bool for a fixed result, or a callable(username) -> bool
        for per-user conditional results.
        """
        self._cooldown_result = result

    def set_update_download_state_error(
        self, request_id: int, error: Exception,
    ) -> None:
        """Make ``update_download_state`` raise ``error`` for one request.

        Also fires through ``update_download_state_if_downloading`` (it
        delegates here), mirroring a production psycopg2 error at the
        UPDATE: the call is recorded but the row is never mutated. Same
        targeted-seam style as ``set_cooldown_result`` /
        ``FakeSlskdUsers.set_directory_error``. Persistent for the
        fake's lifetime (a one-shot harvest only calls once).
        """
        self._update_download_state_errors[request_id] = error

    def set_advisory_lock_result(
        self, result: bool | Callable[[int, int], bool],
    ) -> None:
        """Configure what advisory_lock yields.

        Pass a bool for a fixed result across every (namespace, key), or
        a callable (namespace, key) -> bool for per-lock answers. The
        callable form is needed for issue #133 where one test scenario
        holds the request-lock but releases the release-lock (or vice
        versa) to model the cross-process race between the auto cycle
        and web force-import on the same MBID.
        """
        self._advisory_lock_result = result

    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Iterator[bool]:
        """In-memory stand-in for ``PipelineDB.advisory_lock``.

        Records every ``(namespace, key)`` invocation and yields the
        value set via ``set_advisory_lock_result`` (default ``True``).
        Tests that want to simulate contention flip the flag to ``False``
        before calling the code under test.
        """
        self.advisory_lock_calls.append((namespace, key))
        acquired = (
            self._advisory_lock_result(namespace, key)
            if callable(self._advisory_lock_result)
            else self._advisory_lock_result)
        yield acquired

    # --- import_jobs queue ---

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob:
        validate_job_type(job_type)
        payload = validate_payload(job_type, payload or {})
        if dedupe_key is not None:
            existing = self._get_import_job_by_dedupe_key(dedupe_key)
            if existing is not None:
                return ImportJob.from_row(existing.to_dict(), deduped=True)
        if job_type == IMPORT_JOB_YOUTUBE and request_id is not None:
            for row in self._import_jobs:
                if (
                    row.get("job_type") == IMPORT_JOB_YOUTUBE
                    and row.get("request_id") == request_id
                    and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
                ):
                    raise ValueError(
                        "active youtube_import already exists for "
                        f"request_id={request_id}"
                    )

        self._next_import_job_id += 1
        now = _utcnow()
        row: dict[str, Any] = {
            "id": self._next_import_job_id,
            "job_type": job_type,
            "status": "queued",
            "request_id": request_id,
            "dedupe_key": dedupe_key,
            "payload": copy.deepcopy(payload),
            "result": None,
            "message": message,
            "error": None,
            "attempts": 0,
            "worker_id": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "heartbeat_at": None,
            "completed_at": None,
            "preview_status": IMPORT_JOB_PREVIEW_WAITING,
            "preview_result": None,
            "preview_message": None,
            "preview_error": None,
            "preview_attempts": 0,
            "preview_worker_id": None,
            "preview_started_at": None,
            "preview_heartbeat_at": None,
            "preview_completed_at": None,
            "importable_at": None,
            "candidate_evidence_id": None,
            "expected_request_status": (
                self._requests.get(request_id, {}).get("status")
                if request_id is not None
                else None
            ),
            "beets_launch_authorized_at": None,
            "beets_launch_release_id": None,
            "beets_launch_source_path": None,
            "beets_launch_request_status": None,
            "beets_launch_snapshot_fingerprint": None,
        }
        self._import_jobs.append(row)
        return ImportJob.from_row(copy.deepcopy(row))

    def get_import_job(self, job_id: int) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id:
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def _get_import_job_by_dedupe_key(
        self,
        dedupe_key: str,
        *,
        active_only: bool = True,
    ) -> ImportJob | None:
        rows = [
            row for row in self._import_jobs
            if row.get("dedupe_key") == dedupe_key
            and (
                not active_only
                or row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
            )
        ]
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]), reverse=True)
        return ImportJob.from_row(copy.deepcopy(rows[0])) if rows else None

    def list_import_jobs(
        self,
        *,
        status: str | None = None,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        if status is not None:
            validate_status(status)
        rows = list(self._import_jobs)
        if status is not None:
            rows = [row for row in rows if row.get("status") == status]
        if request_id is not None:
            rows = [row for row in rows if row.get("request_id") == request_id]
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]), reverse=True)
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def list_active_import_jobs(
        self,
        *,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        rows = [
            row for row in self._import_jobs
            if row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
            and (request_id is None or row.get("request_id") == request_id)
        ]
        rows.sort(key=lambda row: (_as_datetime(row.get("created_at")), row["id"]))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def find_active_youtube_import_job(
        self,
        *,
        request_id: int,
        browse_id: str,
    ) -> ImportJob | None:
        rows = [
            row for row in self._import_jobs
            if row.get("job_type") == IMPORT_JOB_YOUTUBE
            and row.get("request_id") == request_id
            and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
        ]
        rows.sort(key=lambda row: row["id"])
        return ImportJob.from_row(copy.deepcopy(rows[0])) if rows else None

    def list_active_youtube_rescues(
        self,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for entry in sorted(
            self.download_logs,
            key=lambda e: (e.created_at, e.id),
        ):
            if entry.source != "youtube" or entry.outcome != "youtube_running":
                continue
            req = self._requests.get(entry.request_id) or {}
            rows.append({
                "download_log_id": entry.id,
                "request_id": entry.request_id,
                "source": entry.source,
                "outcome": entry.outcome,
                "youtube_metadata": copy.deepcopy(entry.youtube_metadata),
                "created_at": entry.created_at,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "mb_release_id": req.get("mb_release_id"),
                "request_status": req.get("status"),
            })
            if len(rows) >= int(limit):
                break
        return rows

    def list_active_import_jobs_for_wrong_match(
        self,
        *,
        download_log_id: int,
        request_id: int | None,
        failed_paths: Iterable[str],
        source_dirs: Iterable[str],
        ignore_import_job_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        paths = {str(path) for path in failed_paths if path}
        dirs = {str(path) for path in source_dirs if path}
        rows: list[dict[str, Any]] = []
        for row in self._import_jobs:
            if row.get("status") not in IMPORT_JOB_ACTIVE_STATUSES:
                continue
            if (
                ignore_import_job_id is not None
                and int(row["id"]) == int(ignore_import_job_id)
            ):
                continue
            payload_raw = row.get("payload")
            payload: dict[str, Any] = payload_raw if isinstance(payload_raw, dict) else {}
            payload_dirs = {
                str(path) for path in payload.get("source_dirs", [])
                if path
            }
            matches = (
                payload.get("download_log_id") == download_log_id
                or str(payload.get("download_log_id")) == str(download_log_id)
                or str(payload.get("failed_path") or "") in paths
                or bool(dirs.intersection(payload_dirs))
            )
            if matches:
                rows.append(row)
        rows.sort(key=lambda row: (_as_datetime(row.get("created_at")), row["id"]))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def count_import_jobs_by_status(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._import_jobs:
            status = str(row.get("status"))
            counts[status] = counts.get(status, 0) + 1
        return counts

    def list_import_job_timeline(self, *, limit: int = 50) -> list[ImportJob]:
        active_rows = [
            row for row in self._import_jobs
            if row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
        ]

        def sort_key(row: dict[str, Any]) -> tuple[int, datetime, datetime, int]:
            status = row.get("status")
            preview_status = row.get("preview_status")
            if status == IMPORT_JOB_RECOVERY_REQUIRED:
                bucket = 0
            elif (
                status == "queued"
                and preview_status in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
            ):
                bucket = 1
            elif status == "running":
                bucket = 2
            elif status == "queued" and preview_status == "running":
                bucket = 3
            elif status == "queued" and preview_status == "waiting":
                bucket = 4
            else:
                bucket = 5
            return (
                bucket,
                _as_datetime(row.get("importable_at")),
                _as_datetime(row.get("created_at")),
                int(row["id"]),
            )

        rows = sorted(active_rows, key=sort_key)
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def claim_next_import_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        queued = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") in IMPORT_JOB_IMPORTABLE_PREVIEW_STATUSES
        ]
        queued.sort(key=lambda row: (
            _as_datetime(row.get("importable_at")),
            _as_datetime(row.get("created_at")),
            row["id"],
        ))
        if not queued:
            return None
        row = queued[0]
        now = _utcnow()
        row["status"] = "running"
        row["attempts"] = int(row.get("attempts") or 0) + 1
        row["worker_id"] = worker_id
        row["started_at"] = row.get("started_at") or now
        row["heartbeat_at"] = now
        row["updated_at"] = now
        return ImportJob.from_row(copy.deepcopy(row))

    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id and row.get("status") in ("queued", "running"):
                now = _utcnow()
                row["status"] = "completed"
                row["result"] = copy.deepcopy(result or {})
                row["message"] = message
                row["error"] = None
                row["completed_at"] = now
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def authorize_import_job_launch(
        self,
        job_id: int,
        *,
        request_id: int,
        release_id: str,
        source_path: str,
    ) -> ImportJob | None:
        request = self._requests.get(request_id)
        for row in self._import_jobs:
            if (
                row["id"] != job_id
                or row.get("status") != "running"
                or row.get("beets_launch_authorized_at") is not None
                or row.get("request_id") != request_id
                or request is None
                or row.get("expected_request_status") is None
                or request.get("status") != row.get("expected_request_status")
                or request.get("status") == "replaced"
                or request.get("mb_release_id") != release_id
            ):
                continue
            evidence_id = row.get("candidate_evidence_id")
            evidence = (
                self._evidence_by_id.get(int(evidence_id))
                if evidence_id is not None else None
            )
            if (
                evidence is None
                or evidence.mb_release_id != release_id
                or evidence.source_path != source_path
                or not evidence.snapshot_fingerprint
            ):
                return None
            job_type = row.get("job_type")
            if job_type == "automation_import":
                state = request.get("active_download_state")
                if (
                    request.get("status") != "downloading"
                    or not isinstance(state, dict)
                    or state.get("current_path") != source_path
                ):
                    return None
            elif job_type == "force_import":
                payload = row.get("payload")
                if (
                    not isinstance(payload, dict)
                    or payload.get("failed_path") != source_path
                ):
                    return None
            elif job_type == "youtube_import":
                payload = row.get("payload")
                if (
                    request.get("status") not in ("wanted", "unsearchable")
                    or not isinstance(payload, dict)
                    or payload.get("staged_path") != source_path
                ):
                    return None
            else:
                return None
            now = _utcnow()
            row["beets_launch_authorized_at"] = now
            row["beets_launch_release_id"] = release_id
            row["beets_launch_source_path"] = source_path
            row["beets_launch_request_status"] = request.get("status")
            row["beets_launch_snapshot_fingerprint"] = (
                evidence.snapshot_fingerprint
            )
            row["updated_at"] = now
            return ImportJob.from_row(copy.deepcopy(row))
        return None

    def mark_import_job_recovery_required(
        self,
        job_id: int,
        *,
        reason: str,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "running"
                and row.get("beets_launch_authorized_at") is not None
            ):
                now = _utcnow()
                row["status"] = IMPORT_JOB_RECOVERY_REQUIRED
                row["message"] = f"Recovery required: {reason}"
                row["error"] = (
                    "Automatic replay refused because Beets may have "
                    "mutated the library"
                )
                row["worker_id"] = None
                row["heartbeat_at"] = None
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def resolve_import_job_recovery(
        self,
        job_id: int,
        *,
        resolution: str,
        reason: str,
    ) -> tuple[ImportJob, ImportJob | None] | None:
        if resolution not in ("retry", "close"):
            raise ValueError(f"Invalid import recovery resolution: {resolution}")
        reason = reason.strip()
        if not reason:
            raise ValueError("Import recovery resolution requires a reason")
        row = next(
            (
                item for item in self._import_jobs
                if item["id"] == job_id
                and item.get("status") == IMPORT_JOB_RECOVERY_REQUIRED
            ),
            None,
        )
        if row is None:
            return None
        original = ImportJob.from_row(copy.deepcopy(row))
        if resolution == "retry":
            request = self._requests.get(int(original.request_id or 0))
            evidence = (
                self._evidence_by_id.get(original.candidate_evidence_id)
                if original.candidate_evidence_id is not None
                else None
            )
            if (
                request is None
                or request.get("status")
                != original.beets_launch_request_status
                or request.get("mb_release_id")
                != original.beets_launch_release_id
                or evidence is None
                or evidence.snapshot_fingerprint
                != original.beets_launch_snapshot_fingerprint
            ):
                return None

            expected_source = None
            if original.job_type == "automation_import":
                state = request.get("active_download_state")
                expected_source = (
                    state.get("current_path")
                    if isinstance(state, dict)
                    else None
                )
            elif original.job_type == "force_import":
                expected_source = original.payload.get("failed_path")
            elif original.job_type == "youtube_import":
                expected_source = original.payload.get("staged_path")
            else:
                return None
            if expected_source != original.beets_launch_source_path:
                return None

        if resolution == "retry" and original.job_type == "automation_import":
            request = self._requests.get(int(original.request_id or 0))
            state = request.get("active_download_state") if request else None
            assert request is not None and isinstance(state, dict)
            state.pop("import_subprocess_started_at", None)
            request["updated_at"] = _utcnow()

        now = _utcnow()
        prior_result = row.get("result")
        merged_result = (
            copy.deepcopy(prior_result)
            if isinstance(prior_result, dict)
            else {}
        )
        merged_result["recovery_resolution"] = {
            "resolution": resolution,
            "reason": reason,
        }
        row["status"] = "failed"
        row["result"] = merged_result
        row["message"] = (
            f"Operator authorized a fresh retry: {reason}"
            if resolution == "retry"
            else f"Operator resolved without replay: {reason}"
        )
        row["error"] = (
            "Ambiguous Beets operation closed before fresh retry"
            if resolution == "retry"
            else "Ambiguous Beets operation closed by operator"
        )
        row["worker_id"] = None
        row["heartbeat_at"] = None
        row["completed_at"] = now
        row["updated_at"] = now
        resolved = ImportJob.from_row(copy.deepcopy(row))

        retry: ImportJob | None = None
        if resolution == "retry":
            retry = self.enqueue_import_job(
                original.job_type,
                request_id=original.request_id,
                dedupe_key=original.dedupe_key,
                payload=original.payload,
                message=f"Operator-authorized retry of recovery job {original.id}",
            )
            retry_row = next(
                item for item in self._import_jobs if item["id"] == retry.id
            )
            retry_row["preview_status"] = original.preview_status
            retry_row["preview_result"] = copy.deepcopy(original.preview_result)
            retry_row["preview_message"] = original.preview_message
            retry_row["preview_error"] = original.preview_error
            retry_row["preview_attempts"] = original.preview_attempts
            retry_row["preview_completed_at"] = original.preview_completed_at
            retry_row["importable_at"] = original.importable_at
            retry_row["candidate_evidence_id"] = original.candidate_evidence_id
            retry = ImportJob.from_row(copy.deepcopy(retry_row))
        return resolved, retry

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id and row.get("status") in ("queued", "running"):
                now = _utcnow()
                row["status"] = "failed"
                row["result"] = copy.deepcopy(result or {})
                row["message"] = message
                row["error"] = error
                row["completed_at"] = now
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def merge_import_job_result(
        self,
        job_id: int,
        patch: dict[str, Any],
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id and row.get("status") in ("completed", "failed"):
                result = row.get("result")
                merged = copy.deepcopy(result) if isinstance(result, dict) else {}
                merged.update(copy.deepcopy(patch))
                row["result"] = merged
                row["updated_at"] = _utcnow()
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def recover_running_import_jobs(
        self,
        *,
        requeue_message: str,
        recovery_message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        running = [
            row for row in self._import_jobs
            if row.get("status") == "running"
        ]
        running.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in running[:limit]:
            now = _utcnow()
            launched = row.get("beets_launch_authorized_at") is not None
            row["status"] = (
                IMPORT_JOB_RECOVERY_REQUIRED if launched else "queued"
            )
            row["message"] = recovery_message if launched else requeue_message
            row["error"] = (
                "Automatic replay refused because Beets may have mutated "
                "the library"
                if launched else None
            )
            row["worker_id"] = None
            if not launched:
                row["started_at"] = None
            row["heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

    def requeue_import_job_for_preview(
        self,
        job_id: int,
        *,
        reason: str,
    ) -> ImportJob | None:
        """Fake mirror of PipelineDB.requeue_import_job_for_preview.

        Only matches rows currently in ``status='running'``. Clears writer
        state, sets preview_status='waiting', preserves attempt counters.
        """
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "running"
                and row.get("beets_launch_authorized_at") is None
            ):
                now = _utcnow()
                row["status"] = "queued"
                row["preview_status"] = IMPORT_JOB_PREVIEW_WAITING
                row["message"] = reason
                row["error"] = None
                row["worker_id"] = None
                row["started_at"] = None
                row["heartbeat_at"] = None
                row["preview_message"] = None
                row["preview_error"] = None
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def claim_next_import_preview_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        queued = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") == "waiting"
        ]
        queued.sort(key=lambda row: (_as_datetime(row.get("created_at")), row["id"]))
        if not queued:
            return None
        row = queued[0]
        now = _utcnow()
        row["preview_status"] = "running"
        row["preview_attempts"] = int(row.get("preview_attempts") or 0) + 1
        row["preview_worker_id"] = worker_id
        row["preview_started_at"] = row.get("preview_started_at") or now
        row["preview_heartbeat_at"] = now
        row["preview_message"] = None
        row["preview_error"] = None
        row["updated_at"] = now
        return ImportJob.from_row(copy.deepcopy(row))

    def heartbeat_import_job_preview(self, job_id: int) -> bool:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "queued"
                and row.get("preview_status") == "running"
            ):
                now = _utcnow()
                row["preview_heartbeat_at"] = now
                row["updated_at"] = now
                return True
        return False

    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "queued"
                and row.get("preview_status") in ("waiting", "running")
            ):
                now = _utcnow()
                row["preview_status"] = IMPORT_JOB_PREVIEW_EVIDENCE_READY
                row["preview_result"] = copy.deepcopy(preview_result or {})
                row["preview_message"] = message
                row["preview_error"] = None
                row["preview_completed_at"] = now
                row["importable_at"] = row.get("importable_at") or now
                row["preview_worker_id"] = None
                row["preview_heartbeat_at"] = None
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        validate_preview_failure_status(preview_status)
        result = copy.deepcopy(preview_result or {})
        for row in self._import_jobs:
            if (
                row["id"] == job_id
                and row.get("status") == "queued"
                and row.get("preview_status") in ("waiting", "running")
            ):
                now = _utcnow()
                row["status"] = "failed"
                row["preview_status"] = preview_status
                row["preview_result"] = result
                row["preview_message"] = message
                row["preview_error"] = error
                row["result"] = {"preview": copy.deepcopy(result)}
                row["message"] = message
                row["error"] = error
                row["preview_completed_at"] = now
                row["completed_at"] = now
                row["preview_worker_id"] = None
                row["preview_heartbeat_at"] = None
                row["updated_at"] = now
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = _utcnow() - older_than
        stale = []
        for row in self._import_jobs:
            if row.get("status") != "queued" or row.get("preview_status") != "running":
                continue
            last = _as_datetime(
                row.get("preview_heartbeat_at")
                or row.get("preview_started_at")
                or row.get("updated_at")
            )
            if last < cutoff:
                stale.append(row)
        stale.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in stale[:limit]:
            now = _utcnow()
            row["preview_status"] = "waiting"
            row["preview_message"] = message
            row["preview_error"] = None
            row["preview_worker_id"] = None
            row["preview_started_at"] = None
            row["preview_heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

    def requeue_running_import_preview_jobs(
        self,
        *,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        running = [
            row for row in self._import_jobs
            if row.get("status") == "queued"
            and row.get("preview_status") == "running"
        ]
        running.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        updated_jobs = []
        for row in running[:limit]:
            now = _utcnow()
            row["preview_status"] = "waiting"
            row["preview_message"] = message
            row["preview_error"] = None
            row["preview_worker_id"] = None
            row["preview_started_at"] = None
            row["preview_heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

    # --- PipelineDB interface methods ---

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        return copy.deepcopy(self._requests.get(request_id))

    def _terminal_state_snapshot(self) -> tuple[object, ...]:
        return copy.deepcopy((
            self._requests,
            self._import_jobs,
            self.download_logs,
            self.denylist,
            self.user_cooldowns,
            self.status_history,
            self.recorded_attempts,
            self.cooldowns_applied,
        ))

    def _restore_terminal_state(self, snapshot: tuple[object, ...]) -> None:
        (
            self._requests,
            self._import_jobs,
            self.download_logs,
            self.denylist,
            self.user_cooldowns,
            self.status_history,
            self.recorded_attempts,
            self.cooldowns_applied,
        ) = cast(Any, snapshot)

    def _terminal_outcome_write_boundary(self, index: int, label: str) -> None:
        del index, label

    def _apply_terminal_request_transition(
        self,
        transition_db: _FakeTerminalTransitionsDB,
        request_id: int,
        transition: transitions.RequestTransition,
        *,
        operator_stop_was_current: bool,
        successful_terminal_acceptance: bool,
    ) -> tuple[transitions.TransitionApplied, ...]:
        preserve_stop = (
            operator_stop_was_current
            and not successful_terminal_acceptance
        )
        if not preserve_stop:
            return (transitions.require_transition_applied(
                transitions.finalize_request(
                    transition_db,
                    request_id,
                    transition,
                )
            ),)
        row = transition_db.get_request(request_id)
        if row is None:
            return (transitions.require_transition_applied(
                transitions.finalize_request(
                    transition_db,
                    request_id,
                    transition,
                )
            ),)
        current_status = str(row["status"])
        applied: list[transitions.TransitionApplied] = []
        if transition.target_status == "wanted":
            has_policy_effect = bool(transition.fields) or (
                transition.attempt_type is not None
            )
            if not has_policy_effect:
                return ()
            if not transition_db.apply_wanted_policy_without_requeue(
                request_id,
                expected_status=current_status,
                fields=dict(transition.fields),
                attempt_type=transition.attempt_type,
            ):
                raise RuntimeError(
                    "locked operator-stop row changed during terminal policy"
                )
            applied.append(transitions.TransitionApplied(
                request_id=request_id,
                from_status=current_status,
                target_status=current_status,
            ))
            return tuple(applied)
        if transition.attempt_type is not None:
            raise ValueError(
                f"{transition.target_status} transition cannot record an attempt"
            )
        if transition.target_status in {"imported", "unsearchable"}:
            if not transition.fields:
                return ()
            if not transition_db.apply_terminal_metadata_without_transition(
                request_id,
                expected_status=current_status,
                fields=dict(transition.fields),
            ):
                raise RuntimeError(
                    "locked operator-stop row changed during terminal metadata"
                )
            return (transitions.TransitionApplied(
                request_id=request_id,
                from_status=current_status,
                target_status=current_status,
            ),)
        return (transitions.require_transition_applied(
            transitions.finalize_request(
                transition_db,
                request_id,
                transition,
            )
        ),)

    def persist_import_terminal_outcome(
        self,
        command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult:
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            applied = []
            locked_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
            )
            operator_stop_was_current = operator_search_stop_is_current(
                locked_status
            )
            if command.initial_transition is not None:
                if (
                    operator_stop_was_current
                    and not command.successful_terminal_acceptance
                    and command.initial_transition.from_status is not None
                    and command.initial_transition.from_status != locked_status
                ):
                    transitions.require_transition_applied(
                        transitions.finalize_request(
                            transition_db,
                            command.request_id,
                            command.initial_transition,
                        )
                    )
                applied.extend(self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    command.initial_transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=(
                        command.successful_terminal_acceptance
                    ),
                ))
            download_log_id = cast(Any, self.log_download)(
                request_id=command.request_id,
                **command.audit.as_log_kwargs(),
            )
            self.set_download_log_candidate_evidence(
                download_log_id,
                self.get_import_job_candidate_evidence_id(command.import_job_id),
            )
            boundary("download_log")
            for transition in command.post_audit_transitions:
                applied.extend(self._apply_terminal_request_transition(
                    transition_db,
                    command.request_id,
                    transition,
                    operator_stop_was_current=operator_stop_was_current,
                    successful_terminal_acceptance=(
                        command.successful_terminal_acceptance
                    ),
                ))
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            for entry in command.cooldowns:
                if self.check_and_apply_cooldown(entry.username):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            if command.job.status == "completed":
                job = self.mark_import_job_completed(
                    command.import_job_id,
                    result=command.job.result,
                    message=command.job.message,
                )
            else:
                assert command.job.error is not None
                job = self.mark_import_job_failed(
                    command.import_job_id,
                    error=command.job.error,
                    result=command.job.result,
                    message=command.job.message,
                )
            if job is None or job.request_id != command.request_id:
                raise RuntimeError("import job terminal compare-and-set failed")
            boundary(f"import_job.{command.job.status}")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_import_terminal_outcome_calls.append(command)
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=tuple(applied),
            cooled_down_users=frozenset(cooled),
        )

    def persist_preview_terminal_outcome(
        self,
        command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult:
        snapshot = self._terminal_state_snapshot()
        boundary_index = 0

        def boundary(label: str) -> None:
            nonlocal boundary_index
            boundary_index += 1
            self._terminal_outcome_write_boundary(boundary_index, label)

        try:
            transition_db = _FakeTerminalTransitionsDB(self, boundary)
            applied = []
            current_status = (
                str(self._requests[command.request_id]["status"])
                if command.request_id in self._requests
                else None
            )
            preserve_current = (
                command.request_transition is not None
                and command.request_transition.target_status == "wanted"
                and operator_search_stop_is_current(current_status)
            )
            if command.request_transition is not None and not preserve_current:
                applied.append(transitions.require_transition_applied(
                    transitions.finalize_request(
                        transition_db,
                        command.request_id,
                        command.request_transition,
                    )
                ))
            download_log_id = cast(Any, self.log_download)(
                request_id=command.request_id,
                **command.audit.as_log_kwargs(),
            )
            self.set_download_log_candidate_evidence(
                download_log_id,
                self.get_import_job_candidate_evidence_id(command.import_job_id),
            )
            boundary("download_log")
            cooled: set[str] = set()
            for entry in command.denylists:
                denied_before = len(self.denylist)
                self.add_denylist(
                    command.request_id,
                    entry.username,
                    entry.reason,
                )
                if len(self.denylist) > denied_before:
                    boundary("denylist")
                if entry.apply_cooldown and self.check_and_apply_cooldown(
                    entry.username
                ):
                    cfg = CooldownConfig()
                    self.add_cooldown(
                        entry.username,
                        _utcnow() + timedelta(days=cfg.cooldown_days),
                        f"{cfg.failure_threshold} consecutive failures",
                    )
                    cooled.add(entry.username)
                    boundary("cooldown")
            job = self.mark_import_job_preview_failed(
                command.import_job_id,
                preview_status=command.preview_status,
                error=command.error,
                preview_result=command.preview_result,
                message=command.message,
            )
            if job is None or job.request_id != command.request_id:
                raise RuntimeError("preview job terminal compare-and-set failed")
            boundary("import_job.preview_failed")
        except Exception:
            self._restore_terminal_state(snapshot)
            raise
        self.persist_preview_terminal_outcome_calls.append(command)
        return TerminalOutcomeResult(
            download_log_id=download_log_id,
            job=job,
            transitions=tuple(applied),
            cooled_down_users=frozenset(cooled),
        )

    def get_request_by_mb_release_id(self, mb_release_id: str) -> dict[str, Any] | None:
        for row in self._requests.values():
            if row.get("mb_release_id") == mb_release_id:
                return copy.deepcopy(row)
        return None

    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> dict[str, Any] | None:
        for row in self._requests.values():
            if row.get("discogs_release_id") == discogs_release_id:
                return copy.deepcopy(row)
        return None

    def get_request_by_release_id(self, release_id: object | None) -> dict[str, Any] | None:
        normalized = normalize_release_id(release_id)
        if not normalized:
            return None

        identity = ReleaseIdentity.from_fields(normalized)
        if identity is None:
            return self.get_request_by_mb_release_id(normalized)

        if identity.source == "musicbrainz":
            return self.get_request_by_mb_release_id(identity.release_id)

        req = self.get_request_by_discogs_release_id(identity.release_id)
        if req:
            return req
        return self.get_request_by_mb_release_id(identity.release_id)

    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        if status == "replaced":
            raise ValueError(
                "status='replaced' is owned by supersede_request_mbid")
        validate_request_metadata_fields(dict(extra))
        row = self._requests.get(request_id)
        if row is None or row.get("status") == "replaced":
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        row["status"] = status
        row["active_download_state"] = None
        row["updated_at"] = _utcnow()
        for key, val in extra.items():
            row[key] = val
        self.status_history.append((request_id, status))
        return True

    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        row = self._requests.get(request_id)
        return bool(
            row is not None
            and row.get("status") == expected_status
            and row.get("status") != "replaced"
        )

    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        """Mirror ``PipelineDB.mark_imported_with_rescue`` (U14).

        Atomic in-memory equivalent: writes ``status='imported'``,
        clears ``unfindable_category``, and on the FIRST rescue stamps
        ``rescued_at`` + ``prior_unfindable_category``. Reserved
        kwargs the production method rejects are rejected here too.
        """
        rescue_owned = {
            "unfindable_category",
            "unfindable_categorised_at",
        }
        bad_rescue_fields = sorted(set(extra) & rescue_owned)
        if bad_rescue_fields:
            raise ValueError(
                "mark_imported_with_rescue cannot accept rescue-owned fields: "
                + ", ".join(bad_rescue_fields)
            )
        validate_request_metadata_fields(dict(extra))
        row = self._requests.get(request_id)
        if row is None or row.get("status") == "replaced":
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        now = _utcnow()
        current_category = row.get("unfindable_category")
        already_rescued = row.get("rescued_at") is not None

        row["status"] = "imported"
        row["active_download_state"] = None
        row["updated_at"] = now
        if current_category is not None:
            row["unfindable_category"] = None
            row["unfindable_categorised_at"] = now
        if current_category is not None and not already_rescued:
            row["rescued_at"] = now
            row["prior_unfindable_category"] = current_category
        for key, val in extra.items():
            row[key] = val
        self.status_history.append((request_id, "imported"))
        return True

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
            }
        )
        if unknown:
            raise ValueError(
                "reset_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        row = self._requests.get(request_id)
        if row is None or row.get("status") == "replaced":
            return False
        source_status = expected_status or str(row["status"])
        if row["status"] != source_status:
            return False
        now = _utcnow()
        row["status"] = "wanted"
        if clear_retry_counters:
            row["search_attempts"] = 0
            row["download_attempts"] = 0
            row["validation_attempts"] = 0
            row["next_retry_after"] = None
            row["last_attempt_at"] = None
        row["active_download_state"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "prev_min_bitrate" in fields:
            row["prev_min_bitrate"] = fields["prev_min_bitrate"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if (
                "prev_min_bitrate" not in fields
                and current_min_bitrate is not None
            ):
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        self.status_history.append((request_id, "wanted"))
        return True

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
            }
        )
        if unknown:
            raise ValueError(
                "reset_downloading_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        row = self._requests.get(request_id)
        if (
            row is None
            or expected_status != "downloading"
            or row["status"] != expected_status
        ):
            return False
        now = _utcnow()
        row["status"] = "wanted"
        row["active_download_state"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "prev_min_bitrate" in fields:
            row["prev_min_bitrate"] = fields["prev_min_bitrate"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if (
                "prev_min_bitrate" not in fields
                and current_min_bitrate is not None
            ):
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        self.status_history.append((request_id, "wanted"))
        return True

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        row = self._requests.get(request_id)
        if (
            row is None
            or expected_status != "wanted"
            or row["status"] != expected_status
        ):
            return False
        now = _utcnow()
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        row["last_attempt_at"] = now
        row["updated_at"] = now
        self.status_history.append((request_id, "downloading"))
        return True

    def set_downloading_if_plan_current(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Mirror of ``PipelineDB.set_downloading_if_plan_current``.

        Atomic plan-aware claim; refuses if status moved off ``wanted``
        OR the plan/ordinal/cycle no longer match the snapshot.
        """
        row = self._requests.get(request_id)
        if row is None or row["status"] != "wanted":
            return False
        if row.get("active_plan_id") != plan_id:
            return False
        if int(row.get("next_plan_ordinal") or 0) != plan_ordinal:
            return False
        if int(row.get("plan_cycle_count") or 0) != cycle_count_snapshot:
            return False
        now = _utcnow()
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        row["last_attempt_at"] = now
        row["updated_at"] = now
        self.status_history.append((request_id, "downloading"))
        return True

    def update_download_state(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "downloading",
    ) -> bool:
        row = self._requests.get(request_id)
        self.update_download_state_calls.append((request_id, state_json))
        injected = self._update_download_state_errors.get(request_id)
        if injected is not None:
            # Mirror a psycopg2 error at the UPDATE: the attempt is
            # recorded (like FakeSlskdAPI's failing calls) but the row
            # is never mutated. See set_update_download_state_error.
            raise injected
        if row and row.get("status") == expected_status:
            try:
                row["active_download_state"] = json.loads(state_json)
            except json.JSONDecodeError:
                row["active_download_state"] = state_json
            row["updated_at"] = _utcnow()
            return True
        return False

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
    ) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "downloading":
            return False
        return self.update_download_state(
            request_id,
            state_json,
            expected_status="downloading",
        )

    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> bool:
        self.update_download_state_current_path_calls.append(
            (request_id, current_path),
        )
        row = self._requests.get(request_id)
        if (
            row
            and row.get("status") == "downloading"
            and row.get("active_download_state") is not None
        ):
            state = row.get("active_download_state")
            if isinstance(state, str):
                try:
                    state = json.loads(state)
                except json.JSONDecodeError:
                    state = {}
            if not isinstance(state, dict):
                state = {}
            state["current_path"] = current_path
            row["active_download_state"] = state
            row["updated_at"] = _utcnow()
            return True
        return False

    def mark_import_subprocess_started(
        self,
        request_id: int,
        timestamp: str,
    ) -> bool:
        """Stamp ``import_subprocess_started_at`` on the active download
        state. No-op when the row has no ``active_download_state``
        (force-import path). See ``docs/advisory-locks.md``.
        """
        self.mark_import_subprocess_started_calls.append(
            (request_id, timestamp),
        )
        row = self._requests.get(request_id)
        if (
            not row
            or row.get("status") != "downloading"
            or row.get("active_download_state") is None
        ):
            return False
        state = row.get("active_download_state")
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except json.JSONDecodeError:
                return False
        if not isinstance(state, dict):
            return False
        state["import_subprocess_started_at"] = timestamp
        row["active_download_state"] = state
        row["updated_at"] = _utcnow()
        return True

    def log_download(self, request_id: int,
                     soulseek_username: str | None = None,
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
                     **extra: Any) -> int:
        """Record a download_log row.

        Every parameter name matches ``PipelineDB.log_download`` exactly
        — the contract test in ``test_fakes.py`` enforces this. Only
        the 12 "first-class" fields land on ``DownloadLogRow``; the
        remaining named fields plus any test-only ``**extra`` merge into
        ``.extra`` so ``assert_log`` can still introspect them.
        """
        if request_id is None:
            # Mirror production: download_log.request_id is NOT NULL
            # (test-fidelity Rule B — the fake must not be more
            # permissive than the real INSERT).
            import psycopg2.errors

            raise psycopg2.errors.NotNullViolation(
                'null value in column "request_id" of relation '
                '"download_log" violates not-null constraint'
            )
        if outcome is not None and outcome not in DOWNLOAD_LOG_OUTCOMES:
            # Mirror download_log_outcome_check (migration 037) — a fake
            # that accepts any string shipped an outcome production
            # rejects (#146 phase-3 grace escape, 2026-07-02).
            import psycopg2.errors

            raise psycopg2.errors.CheckViolation(
                'new row for relation "download_log" violates check '
                f'constraint "download_log_outcome_check" (outcome={outcome!r})'
            )
        beets_distance_value, beets_scenario_value = derive_validation_log_columns(
            validation_result,
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
        )
        new_log_id = self._mint_download_log_id()
        auxiliary: dict[str, Any] = {
            "download_path": download_path,
            "valid": valid,
            "bitrate": bitrate,
            "sample_rate": sample_rate,
            "bit_depth": bit_depth,
            "is_vbr": is_vbr,
            "was_converted": was_converted,
            "original_filetype": original_filetype,
            "slskd_filetype": slskd_filetype,
            "actual_filetype": actual_filetype,
            "actual_min_bitrate": actual_min_bitrate,
            "spectral_grade": spectral_grade,
            "spectral_bitrate": spectral_bitrate,
            "existing_min_bitrate": existing_min_bitrate,
            "existing_spectral_bitrate": existing_spectral_bitrate,
            "final_format": final_format,
            "v0_probe_kind": v0_probe_kind,
            "v0_probe_min_bitrate": v0_probe_min_bitrate,
            "v0_probe_avg_bitrate": v0_probe_avg_bitrate,
            "v0_probe_median_bitrate": v0_probe_median_bitrate,
            "existing_v0_probe_kind": existing_v0_probe_kind,
            "existing_v0_probe_min_bitrate": existing_v0_probe_min_bitrate,
            "existing_v0_probe_avg_bitrate": existing_v0_probe_avg_bitrate,
            "existing_v0_probe_median_bitrate": existing_v0_probe_median_bitrate,
        }
        auxiliary.update(extra)
        self.download_logs.append(DownloadLogRow(
            request_id=request_id,
            outcome=outcome,
            soulseek_username=soulseek_username,
            filetype=filetype,
            beets_distance=beets_distance_value,
            beets_scenario=beets_scenario_value,
            beets_detail=beets_detail,
            staged_path=staged_path,
            error_message=error_message,
            validation_result=validation_result,
            import_result=import_result,
            transfer_detail=transfer_detail,
            id=new_log_id,
            source_download_log_id=source_download_log_id,
            extra=auxiliary,
        ))
        return new_log_id

    # --- YouTube rescue ingest (mirrors PipelineDB U2 methods) ---

    _YOUTUBE_TERMINAL_OUTCOMES: frozenset[str] = frozenset({
        "youtube_success", "youtube_failed",
    })

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: str | None,
        yt_url: str,
        expected_track_count: int,
        resolver_mapping_id: int | None = None,
        per_track_video_ids: list[str] | None = None,
    ) -> int:
        """Mirror of ``PipelineDB.insert_youtube_running``.

        Raises ``YoutubeInFlightError`` when a ``youtube_running`` row
        already exists for the same ``request_id``, mirroring the real
        partial unique index (``one_youtube_running_per_request``) on
        the production schema.
        """
        existing_id: int | None = None
        for entry in self.download_logs:
            if (entry.source == "youtube"
                    and entry.outcome == "youtube_running"
                    and entry.request_id == request_id):
                existing_id = entry.id
                break
        if existing_id is not None:
            # Look up YoutubeInFlightError lazily so we always raise the
            # currently-loaded class. A prior test in this run may have
            # done ``importlib.reload(lib.pipeline_db)`` (e.g.
            # ``TestReleaseIdToLockKey::test_key_is_stable_across_imports``);
            # a module-level binding would point at the pre-reload class
            # and assertRaises in the caller would miss it.
            from lib.pipeline_db import YoutubeInFlightError as _YIFE
            raise _YIFE(request_id, existing_id)

        new_log_id = self._mint_download_log_id()
        metadata: dict[str, Any] = {
            "yt_url": yt_url,
            "browse_id": browse_id,
            "audio_playlist_id": audio_playlist_id,
            "expected_track_count": int(expected_track_count),
        }
        if resolver_mapping_id is not None:
            metadata["resolver_mapping_id"] = int(resolver_mapping_id)
        if per_track_video_ids is not None:
            metadata["per_track_video_ids"] = [
                str(video_id) for video_id in per_track_video_ids
            ]
        self.download_logs.append(DownloadLogRow(
            request_id=request_id,
            outcome="youtube_running",
            source="youtube",
            youtube_metadata=metadata,
            id=new_log_id,
        ))
        return new_log_id

    def enqueue_youtube_import_and_mark_success(
        self,
        *,
        download_log_id: int,
        request_id: int,
        dedupe_key: str,
        payload: dict[str, Any],
        message: str,
        terminal_metadata: dict[str, Any],
    ) -> ImportJob:
        job = self.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=payload,
            message=message,
        )
        self.update_youtube_terminal(
            download_log_id,
            "youtube_success",
            terminal_metadata,
        )
        return job

    def update_youtube_terminal(
        self,
        download_log_id: int,
        outcome: str,
        metadata_dict: dict[str, Any],
    ) -> None:
        """Mirror of ``PipelineDB.update_youtube_terminal``.

        Merges ``metadata_dict`` onto the existing ``youtube_metadata``
        blob the way the production ``||`` JSONB operator would.
        """
        if outcome not in self._YOUTUBE_TERMINAL_OUTCOMES:
            raise ValueError(
                f"update_youtube_terminal: outcome must be one of "
                f"{sorted(self._YOUTUBE_TERMINAL_OUTCOMES)!r}, got {outcome!r}"
            )
        for entry in self.download_logs:
            if entry.id == download_log_id:
                entry.outcome = outcome
                merged: dict[str, Any] = dict(entry.youtube_metadata or {})
                merged.update(metadata_dict)
                entry.youtube_metadata = merged
                return
        # Production UPDATE silently no-ops if the id doesn't exist;
        # mirror that.

    def claim_next_youtube_pending(
        self,
        *,
        worker_id: str | None,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        rows = sorted(
            (entry for entry in self.download_logs
             if entry.source == "youtube"
             and entry.outcome == "youtube_running"
             and not (entry.youtube_metadata or {}).get("worker_claimed_at")),
            key=lambda e: (e.created_at, e.id),
        )[:int(limit)]
        claimed_at = _utcnow().isoformat()
        for entry in rows:
            metadata = dict(entry.youtube_metadata or {})
            metadata["worker_claimed_at"] = claimed_at
            metadata["worker_id"] = worker_id
            entry.youtube_metadata = metadata
        return [
            {
                "id": entry.id,
                "request_id": entry.request_id,
                "source": entry.source,
                "outcome": entry.outcome,
                "youtube_metadata": copy.deepcopy(entry.youtube_metadata)
                if entry.youtube_metadata is not None else None,
                "created_at": entry.created_at,
            }
            for entry in rows
        ]

    def find_orphan_youtube_running(self) -> list[int]:
        """Mirror of ``PipelineDB.find_orphan_youtube_running``."""
        rows = sorted(
            (entry for entry in self.download_logs
             if entry.source == "youtube"
             and entry.outcome == "youtube_running"
             and (entry.youtube_metadata or {}).get("worker_claimed_at")),
            key=lambda e: (e.created_at, e.id),
        )
        return [entry.id for entry in rows]

    def abandon_auto_import_request(
        self,
        *,
        request_id: int,
        current_path: str,
        soulseek_username: str | None,
        filetype: str | None,
        beets_detail: str,
        outcome: str,
        staged_path: str,
        error_message: str,
        validation_result: Any,
    ) -> int | None:
        row = self._requests.get(request_id)
        if row is None or row.get("status") != "downloading":
            return None
        state = row.get("active_download_state")
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except json.JSONDecodeError:
                return None
        if not isinstance(state, dict):
            return None
        if state.get("current_path") != current_path:
            return None
        if state.get("import_subprocess_started_at") is None:
            return None

        now = _utcnow()
        row["status"] = "wanted"
        row["active_download_state"] = None
        row["updated_at"] = now
        self.status_history.append((request_id, "wanted"))
        self.record_attempt(
            request_id,
            "download",
            expected_status="wanted",
        )
        log_kwargs: dict[str, Any] = {}
        if validation_result is None:
            log_kwargs["beets_scenario"] = "abandoned_auto_import"
        return self.log_download(
            request_id=request_id,
            soulseek_username=soulseek_username,
            filetype=filetype,
            beets_detail=beets_detail,
            outcome=outcome,
            staged_path=staged_path,
            error_message=error_message,
            validation_result=validation_result,
            **log_kwargs,
        )

    def add_denylist(self, request_id: int, username: str,
                     reason: str | None = None) -> None:
        if any(
            entry.request_id == request_id and entry.username == username
            for entry in self.denylist
        ):
            return
        self.denylist.append(DenylistEntry(request_id, username, reason))

    def get_denylisted_users(self, request_id: int) -> list[dict[str, Any]]:
        return [
            {"username": e.username, "reason": e.reason, "created_at": None}
            for e in self.denylist if e.request_id == request_id
        ]

    def list_denylist_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "request_id": entry.request_id,
                "username": entry.username,
                "reason": entry.reason,
                "created_at": None,
            }
            for entry in sorted(
                self.denylist,
                key=lambda row: (row.request_id, row.username),
            )
        ]

    # --- bad_audio_hashes ---

    def add_bad_audio_hashes(
        self,
        request_id: int,
        reported_username: str | None,
        reason: str | None,
        hashes: list[BadAudioHashInput],
    ) -> int:
        """Insert bad-rip hashes; dedupe on (hash_value, audio_format)."""
        existing = {
            (row.hash_value, row.audio_format) for row in self.bad_audio_hashes
        }
        inserted = 0
        for h in hashes:
            key = (h.hash_value, h.audio_format)
            if key in existing:
                continue
            existing.add(key)
            self._next_bad_audio_hash_id += 1
            self.bad_audio_hashes.append(BadAudioHashRow(
                id=self._next_bad_audio_hash_id,
                hash_value=h.hash_value,
                audio_format=h.audio_format,
                request_id=request_id,
                reported_username=reported_username,
                reason=reason,
                reported_at=_utcnow(),
            ))
            inserted += 1
        return inserted

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None:
        self.lookup_bad_audio_hash_calls.append((hash_value, audio_format))
        for row in self.bad_audio_hashes:
            if row.hash_value == hash_value and row.audio_format == audio_format:
                return row
        return None

    def has_any_bad_audio_hashes(self) -> bool:
        self.has_any_bad_audio_hashes_calls += 1
        return bool(self.bad_audio_hashes)

    def get_recent_successful_uploader(
        self,
        request_id: int,
    ) -> str | None:
        """Most recent successful uploader for this request, or None."""
        for entry in reversed(self.download_logs):
            if entry.request_id != request_id:
                continue
            if entry.outcome not in ("success", "force_import"):
                continue
            if entry.soulseek_username is None:
                continue
            return entry.soulseek_username
        return None

    def get_active_import_job_for_request(
        self,
        request_id: int,
    ) -> ImportJob | None:
        """Most recent queued/running import job for this request, or None."""
        rows = [
            row for row in self._import_jobs
            if row.get("request_id") == request_id
            and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
        ]
        if not rows:
            return None
        rows.sort(key=lambda row: row["id"], reverse=True)
        return ImportJob.from_row(copy.deepcopy(rows[0]))

    def check_and_apply_cooldown(self, username: str,
                                  config: Any = None) -> bool:  # noqa: ARG002
        self.cooldowns_applied.append(username)
        if callable(self._cooldown_result):
            return self._cooldown_result(username)
        return self._cooldown_result

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        self.recorded_attempts.append((request_id, attempt_type))
        row = self._requests.get(request_id)
        if row and row.get("status") == expected_status != "replaced":
            col = f"{attempt_type}_attempts"
            now = _utcnow()
            row[col] = (row.get(col) or 0) + 1
            row["last_attempt_at"] = now
            row["updated_at"] = now
            backoff_minutes = min(
                BACKOFF_BASE_MINUTES * (2 ** (row[col] - 1)),
                BACKOFF_MAX_MINUTES,
            )
            row["next_retry_after"] = now + timedelta(minutes=backoff_minutes)
            return True
        return False

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> bool:
        """UPSERT a row into ``field_resolutions`` mirroring migration 030.

        On conflict: increment ``attempts``, replace status / reason,
        bump ``resolved_at``. Tests assert directly against the dict.
        """
        request = self._requests.get(int(request_id))
        if request is None or request.get("status") == "replaced":
            return False
        key = (int(request_id), field_name)
        now = _utcnow()
        existing = self.field_resolutions.get(key)
        if existing is None:
            self._next_field_resolution_id += 1
            self.field_resolutions[key] = FieldResolutionRow(
                request_id=int(request_id),
                field_name=field_name,
                status=status,
                reason_code=reason_code,
                attempts=1,
                resolved_at=now,
                id=self._next_field_resolution_id,
            )
            return True
        existing.status = status
        existing.reason_code = reason_code
        existing.attempts += 1
        existing.resolved_at = now
        return True

    def get_field_resolution(
        self,
        request_id: int,
        field_name: str,
    ) -> dict[str, Any] | None:
        """Return the side-table row for ``(request_id, field_name)`` as a dict."""
        row = self.field_resolutions.get((int(request_id), field_name))
        if row is None:
            return None
        return {
            "id": row.id,
            "request_id": row.request_id,
            "field_name": row.field_name,
            "resolved_at": row.resolved_at,
            "status": row.status,
            "reason_code": row.reason_code,
            "attempts": row.attempts,
        }

    # --- Triage cohort (U15) ---------------------------------------------
    #
    # Mirrors the four new ``PipelineDB`` triage methods so the service
    # layer can be exercised without a real Postgres. Each method bumps
    # ``self.query_counts`` exactly once per invocation so the N+1 guard
    # test can assert ``sum(query_counts.values()) <= 5`` across the
    # cohort path.

    def list_triage_page(
        self,
        *,
        filter_spec: Any,
        page_size: int,
        after_request_id: int | None,
    ) -> list[dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.list_triage_page``."""
        self.query_counts["list_triage_page"] = (
            self.query_counts.get("list_triage_page", 0) + 1
        )
        kind = getattr(filter_spec, "kind", None)
        unfindable_category = getattr(filter_spec, "unfindable_category", None)
        field_name = getattr(filter_spec, "field_name", None)
        status_code = getattr(filter_spec, "status_code", None)
        reason_code = getattr(filter_spec, "reason_code", None)

        def keep(row: dict[str, Any]) -> bool:
            if kind == "unfindable":
                if row.get("unfindable_category") is None:
                    return False
                if (unfindable_category is not None
                        and row.get("unfindable_category") != unfindable_category):
                    return False
                return True
            if kind == "data_quality":
                rid = int(row["id"])
                matched = False
                for (resolution_rid, _fname), fr in self.field_resolutions.items():
                    if resolution_rid != rid:
                        continue
                    if not fr.status.startswith("unresolved_"):
                        continue
                    if field_name is not None and fr.field_name != field_name:
                        continue
                    if status_code is not None and fr.status != status_code:
                        continue
                    if reason_code is not None and fr.reason_code != reason_code:
                        continue
                    matched = True
                    break
                return matched
            if kind == "search_not_converting":
                summary = self._compute_search_summary(int(row["id"]))
                return (summary is not None
                        and summary["total_searches"] > 0
                        and summary["found_count"] == 0)
            if kind == "all":
                return True
            raise ValueError(f"unsupported triage filter kind: {kind!r}")

        rows = sorted(
            (r for r in self._requests.values() if keep(r)),
            key=lambda r: int(r["id"]),
        )
        if after_request_id is not None:
            rows = [r for r in rows if int(r["id"]) > int(after_request_id)]
        rows = rows[: int(page_size)]
        # Return projection mirroring the real SELECT list — kept
        # deliberately narrow so tests can't accidentally rely on a
        # column the production page query doesn't include.
        projection_keys = (
            "id", "artist_name", "album_title", "year", "status", "source",
            "mb_release_id", "discogs_release_id", "release_group_year",
            "is_va_compilation", "catalog_number", "failure_class",
            "search_filetype_override", "unfindable_category",
            "unfindable_categorised_at", "last_artist_probe_at",
            "last_artist_probe_match_count", "rescued_at",
            "prior_unfindable_category",
        )
        return [{k: r.get(k) for k in projection_keys} for r in rows]

    def get_field_resolutions_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]:
        """In-memory mirror of ``PipelineDB.get_field_resolutions_for_requests``."""
        self.query_counts["get_field_resolutions_for_requests"] = (
            self.query_counts.get("get_field_resolutions_for_requests", 0) + 1
        )
        wanted = {int(r) for r in request_ids}
        out: dict[int, list[dict[str, Any]]] = {}
        # Order by (request_id, field_name) to mirror the production
        # ORDER BY clause.
        for (rid, _fn), fr in sorted(
            self.field_resolutions.items(), key=lambda kv: kv[0]
        ):
            if rid not in wanted:
                continue
            out.setdefault(rid, []).append({
                "id": fr.id,
                "request_id": fr.request_id,
                "field_name": fr.field_name,
                "resolved_at": fr.resolved_at,
                "status": fr.status,
                "reason_code": fr.reason_code,
                "attempts": fr.attempts,
            })
        return out

    def get_search_summaries_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.get_search_summaries_for_requests``.

        Aggregates ``self.search_logs`` against the same shape the
        production view emits. Requests with zero rows in window are
        omitted (the view excludes empty groups via ``GROUP BY``).
        """
        self.query_counts["get_search_summaries_for_requests"] = (
            self.query_counts.get("get_search_summaries_for_requests", 0) + 1
        )
        out: dict[int, dict[str, Any]] = {}
        for rid in request_ids:
            summary = self._compute_search_summary(int(rid))
            if summary is not None:
                out[int(rid)] = summary
        return out

    def get_recent_search_log_for_requests(
        self,
        request_ids: list[int],
        *,
        per_request_limit: int,
    ) -> dict[int, list[dict[str, Any]]]:
        """In-memory mirror of ``PipelineDB.get_recent_search_log_for_requests``.

        Walks ``self.search_logs`` newest-first and emits at most
        ``per_request_limit`` rows per request id.
        """
        self.query_counts["get_recent_search_log_for_requests"] = (
            self.query_counts.get("get_recent_search_log_for_requests", 0) + 1
        )
        wanted = {int(r) for r in request_ids}
        out: dict[int, list[dict[str, Any]]] = {}
        # Sort by (created_at, id) DESC so the most recent rows come
        # first per request.
        for entry in sorted(
            self.search_logs,
            key=lambda e: (e.created_at, e.id),
            reverse=True,
        ):
            if entry.request_id not in wanted:
                continue
            bucket = out.setdefault(entry.request_id, [])
            if len(bucket) >= int(per_request_limit):
                continue
            bucket.append({
                "id": entry.id,
                "request_id": entry.request_id,
                "created_at": entry.created_at,
                "plan_strategy": entry.plan_strategy,
                "query": entry.query,
                "outcome": entry.outcome,
                "result_count": entry.result_count,
                "rejection_reason": entry.rejection_reason,
                "matcher_score_top1": entry.matcher_score_top1,
            })
        return out

    def _compute_search_summary(
        self, request_id: int,
    ) -> dict[str, Any] | None:
        """Compute one row of the ``request_search_summary`` view.

        Mirrors the SQL aggregate against ``self.search_logs``. Returns
        ``None`` when the request has zero rows — matches the view's
        ``GROUP BY`` semantics (empty groups produce no row).
        """
        rows = [e for e in self.search_logs if e.request_id == int(request_id)]
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
        dominant = (
            max(reason_counts.items(), key=lambda kv: kv[1])[0]
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

    def update_spectral_state(self, request_id: int,
                              update: RequestSpectralStateUpdate) -> bool:
        return self.update_request_fields(
            request_id, **update.as_update_fields(),
        )

    @staticmethod
    def _assert_album_quality_evidence_constraints(
        evidence: AlbumQualityEvidence,
    ) -> None:
        has_lossless_lineage = (
            (
                evidence.v0_metric is not None
                and evidence.v0_metric.subject == EVIDENCE_SUBJECT_SOURCE
            )
            or evidence.verified_lossless_proof is not None
            or (
                evidence.measurement.was_converted_from or ""
            ).lower() in LOSSLESS_CODECS
        )
        if (
            evidence.lineage_version >= 4
            and evidence.measurement.spectral_subject
                == EVIDENCE_SUBJECT_INSTALLED
            and has_lossless_lineage
        ):
            import psycopg2.errors
            raise psycopg2.errors.CheckViolation(
                "violates check constraint "
                '"album_quality_evidence_lossless_lineage_spectral_subject"'
            )

    def _store_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
    ) -> None:
        """Mirror PostgreSQL constraints at every fake evidence write."""
        self._assert_album_quality_evidence_constraints(evidence)
        evidence_id = evidence.id
        if evidence_id is None:
            raise ValueError("stored album quality evidence requires an id")
        stored = copy.deepcopy(evidence)
        key = (stored.mb_release_id, stored.snapshot_fingerprint)
        self.album_quality_evidence[key] = stored
        self._evidence_by_id[evidence_id] = stored

    def upsert_album_quality_evidence(
        self,
        evidence: AlbumQualityEvidence,
    ) -> None:
        evidence = evidence.sorted_for_storage()
        errors = evidence.storage_validation_errors()
        if errors:
            raise ValueError("; ".join(errors))
        key = (evidence.mb_release_id, evidence.snapshot_fingerprint)
        existing = self.album_quality_evidence.get(key)
        incoming_lossless_lineage = (
            (
                evidence.v0_metric is not None
                and evidence.v0_metric.subject == EVIDENCE_SUBJECT_SOURCE
            )
            or evidence.verified_lossless_proof is not None
            or (
                evidence.measurement.was_converted_from or ""
            ).lower() in LOSSLESS_CODECS
        )
        # Spectral is an atomic pair. A stale writer without a grade cannot
        # erase a successful attempt-time scan on the same audio snapshot.
        # R19 is the exception: new lossless lineage clears a stored
        # installed-subject tuple because those derivative bytes are not an
        # authoritative spectral subject.
        if (
            existing is not None
            and existing.lineage_version >= 4
            and existing.measurement.spectral_grade is not None
            and evidence.measurement.spectral_grade is None
            and not (
                incoming_lossless_lineage
                and existing.measurement.spectral_subject
                    == EVIDENCE_SUBJECT_INSTALLED
            )
        ):
            evidence = msgspec.structs.replace(
                evidence,
                measurement=msgspec.structs.replace(
                    evidence.measurement,
                    spectral_grade=existing.measurement.spectral_grade,
                    spectral_bitrate_kbps=(
                        existing.measurement.spectral_bitrate_kbps
                    ),
                    spectral_subject=existing.measurement.spectral_subject,
                    spectral_provenance=(
                        existing.measurement.spectral_provenance
                    ),
                ),
            )
        # V0 is an atomic tuple. A stale writer with no metric preserves the
        # whole stored fact; a valid incoming metric replaces it wholesale.
        if (
            existing is not None
            and existing.lineage_version >= 4
            and existing.v0_metric is not None
            and evidence.v0_metric is None
        ):
            evidence = msgspec.structs.replace(
                evidence,
                v0_metric=existing.v0_metric,
            )
        if (
            existing is not None
            and existing.on_disk_v0_research_attempted
            and not evidence.on_disk_v0_research_attempted
        ):
            evidence = msgspec.structs.replace(
                evidence,
                on_disk_v0_research_attempted=True,
            )
        if (
            existing is not None
            and existing.current_enrichment_required
            and not evidence.current_enrichment_required
        ):
            evidence = msgspec.structs.replace(
                evidence,
                current_enrichment_required=True,
            )
        if existing is not None and existing.id is not None:
            evidence_id = existing.id
        else:
            self._next_evidence_id += 1
            evidence_id = self._next_evidence_id
        self._store_album_quality_evidence(
            msgspec.structs.replace(evidence, id=evidence_id)
        )

    def load_album_quality_evidence_by_id(
        self,
        evidence_id: int | None,
    ) -> AlbumQualityEvidence | None:
        if evidence_id is None:
            return None
        evidence = self._evidence_by_id.get(int(evidence_id))
        return copy.deepcopy(evidence) if evidence is not None else None

    def find_album_quality_evidence(
        self,
        *,
        mb_release_id: str,
        snapshot_fingerprint: str,
    ) -> AlbumQualityEvidence | None:
        evidence = self.album_quality_evidence.get(
            (mb_release_id, snapshot_fingerprint)
        )
        return copy.deepcopy(evidence) if evidence is not None else None

    def claim_current_v0_research_attempt(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or evidence.v0_metric is not None
            or evidence.on_disk_v0_research_attempted
        ):
            return False
        claimed = msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=True,
        )
        self._store_album_quality_evidence(claimed)
        return True

    def persist_current_spectral_measurement(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        grade: str,
        bitrate_kbps: int | None,
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or evidence.measurement.spectral_grade is not None
            or evidence.measurement.spectral_bitrate_kbps is not None
        ):
            return False
        measurement = msgspec.structs.replace(
            evidence.measurement,
            spectral_grade=grade,
            spectral_bitrate_kbps=bitrate_kbps,
            spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
        completed = msgspec.structs.replace(
            evidence,
            measurement=measurement,
        )
        self._store_album_quality_evidence(completed)
        return True

    def persist_current_v0_research_metric(
        self,
        *,
        request_id: int,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
        metric: AlbumQualityV0Metric,
    ) -> bool:
        request = self._requests.get(int(request_id))
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            request is None
            or request.get("current_evidence_id") != int(expected_evidence_id)
            or evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or not evidence.on_disk_v0_research_attempted
            or evidence.v0_metric is not None
        ):
            return False
        completed = msgspec.structs.replace(
            evidence,
            v0_metric=metric,
        )
        self._store_album_quality_evidence(completed)
        return True

    def release_current_v0_research_attempt(
        self,
        *,
        expected_evidence_id: int,
        expected_snapshot_fingerprint: str,
    ) -> bool:
        evidence = self._evidence_by_id.get(int(expected_evidence_id))
        if (
            evidence is None
            or evidence.snapshot_fingerprint != expected_snapshot_fingerprint
            or not evidence.on_disk_v0_research_attempted
            or evidence.v0_metric is not None
        ):
            return False
        released = msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=False,
        )
        self._store_album_quality_evidence(released)
        return True

    def set_import_job_candidate_evidence(
        self,
        import_job_id: int,
        evidence_id: int | None,
    ) -> None:
        for row in self._import_jobs:
            if row.get("id") == import_job_id:
                row["candidate_evidence_id"] = evidence_id
                row["updated_at"] = _utcnow()
                return

    def set_download_log_candidate_evidence(
        self,
        download_log_id: int,
        evidence_id: int | None,
    ) -> None:
        for row in self.download_logs:
            if row.id == download_log_id:
                row.candidate_evidence_id = evidence_id
                return

    def set_request_current_evidence(
        self,
        request_id: int,
        evidence_id: int | None,
        *,
        expected_status: str | None = None,
    ) -> bool:
        row = self._requests.get(request_id)
        if (
            row is not None
            and row.get("status") != "replaced"
            and (
                expected_status is None
                or row.get("status") == expected_status
            )
        ):
            row["current_evidence_id"] = evidence_id
            row["updated_at"] = _utcnow()
            return True
        return False

    def get_import_job_candidate_evidence_id(
        self,
        import_job_id: int,
    ) -> int | None:
        for row in self._import_jobs:
            if row.get("id") == import_job_id:
                val = row.get("candidate_evidence_id")
                return int(val) if val is not None else None
        return None

    def get_download_log_candidate_evidence_id(
        self,
        download_log_id: int,
    ) -> int | None:
        for row in self.download_logs:
            if row.id == download_log_id:
                return (
                    int(row.candidate_evidence_id)
                    if row.candidate_evidence_id is not None
                    else None
                )
        return None

    def get_request_current_evidence_id(
        self,
        request_id: int,
    ) -> int | None:
        row = self._requests.get(request_id)
        if row is None:
            return None
        val = row.get("current_evidence_id")
        return int(val) if val is not None else None

    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        self.clear_on_disk_quality_fields_calls.append(request_id)
        row = self._requests.get(request_id)
        if row is None or row.get("status") == "replaced":
            return
        row["verified_lossless"] = False
        row["current_spectral_grade"] = None
        row["current_spectral_bitrate"] = None
        row["current_lossless_source_v0_probe_min_bitrate"] = None
        row["current_lossless_source_v0_probe_avg_bitrate"] = None
        row["current_lossless_source_v0_probe_median_bitrate"] = None
        row["current_evidence_id"] = None
        row["imported_path"] = None
        row["updated_at"] = _utcnow()

    def get_downloading(self) -> list[dict[str, Any]]:
        return [copy.deepcopy(r) for r in self._requests.values()
                if r.get("status") == "downloading"]

    def update_request_fields(
        self,
        request_id: int,
        **fields: Any,
    ) -> bool:
        expected_status_raw = fields.pop("expected_status", None)
        if (
            expected_status_raw is not None
            and not isinstance(expected_status_raw, str)
        ):
            raise TypeError("expected_status must be a string or None")
        expected_status = expected_status_raw
        validate_request_metadata_fields(dict(fields))
        self.update_request_fields_calls.append((request_id, dict(fields)))
        row = self._requests.get(request_id)
        if (
            not row
            or row.get("status") == "replaced"
            or (
                expected_status is not None
                and row.get("status") != expected_status
            )
        ):
            return False
        if not fields:
            # Mirror production's control-only CAS: validate the row and
            # expected status, but do not manufacture an ``updated_at`` write.
            return True
        if fields.get("mb_release_id") is not None:
            # Production's UPDATE hits the same UNIQUE(mb_release_id)
            # as the INSERT — re-pointing a row at another row's mbid
            # raises there too (setting a row's own mbid is a no-op).
            self._assert_mb_release_id_unique(
                fields["mb_release_id"], exclude_id=request_id)
        row.update(fields)
        row["updated_at"] = _utcnow()
        return True

    # --- Unfindable detection (U13) ---
    #
    # Each fake mirrors the production PipelineDB writer's contract:
    # one statement, no cursor mutation, autocommit-safe. Tests assert
    # against the persisted row state (and per-method call recorders
    # for the R20 cursor-isolation runtime guard).

    def list_unfindable_probe_candidates(
        self,
        *,
        limit: int,
        probe_interval_days: int,
    ) -> list[dict[str, Any]]:
        """Mirror PipelineDB.list_unfindable_probe_candidates.

        Pulls ``status='wanted'`` rows whose ``last_artist_probe_at``
        is NULL or older than ``probe_interval_days``, oldest first
        (NULL sorts before any timestamp).
        """
        if limit <= 0:
            return []
        cutoff = _utcnow() - timedelta(days=int(probe_interval_days))
        eligible: list[dict[str, Any]] = []
        for row in self._requests.values():
            if row.get("status") != "wanted":
                continue
            last = row.get("last_artist_probe_at")
            if last is not None:
                last_dt = _as_datetime(last)
                if last_dt > cutoff:
                    continue
            eligible.append({
                "id": row["id"],
                "artist_name": row.get("artist_name"),
                "unfindable_category": row.get("unfindable_category"),
                "last_artist_probe_at": row.get("last_artist_probe_at"),
                "last_artist_probe_match_count": row.get(
                    "last_artist_probe_match_count"),
            })

        def _sort_key(r: dict[str, Any]) -> tuple[int, datetime, int]:
            ts = r["last_artist_probe_at"]
            if ts is None:
                return (0, datetime.min.replace(tzinfo=timezone.utc),
                        int(r["id"]))
            return (1, _as_datetime(ts), int(r["id"]))
        eligible.sort(key=_sort_key)
        return eligible[: int(limit)]

    def record_artist_probe(
        self,
        request_id: int,
        *,
        match_count: int,
        observed_at: datetime,
    ) -> None:
        """Mirror PipelineDB.record_artist_probe.

        Mirrors the ``AND status='wanted'`` guard from production: if
        the row has transitioned out of ``wanted`` (e.g. a concurrent
        ``mark_imported_with_rescue`` flipped it to ``imported`` while
        the probe was inflight), the write is a silent no-op. The call
        is still recorded on ``record_artist_probe_calls`` because tests
        need to see the attempt happened.
        """
        self.record_artist_probe_calls.append(
            (request_id, int(match_count), observed_at),
        )
        row = self._requests.get(request_id)
        if row is None:
            return
        if row.get("status") != "wanted":
            return
        row["last_artist_probe_at"] = observed_at
        row["last_artist_probe_match_count"] = int(match_count)
        row["updated_at"] = observed_at

    def set_unfindable_category(
        self,
        request_id: int,
        *,
        category: str | None,
        categorised_at: datetime,
    ) -> None:
        """Mirror PipelineDB.set_unfindable_category.

        Enforces the same 4-category vocabulary the production CHECK
        constraint guards. ``None`` clears the column.

        Mirrors the ``AND status='wanted'`` guard from production: if a
        concurrent rescue flipped the row out of ``wanted`` mid-probe,
        the late verdict write is a silent no-op. The call is still
        recorded on ``set_unfindable_category_calls`` so tests can see
        the attempt happened.
        """
        valid = {
            "artist_absent",
            "album_absent_artist_present",
            "one_track_structural",
            "wrong_pressing_available",
        }
        if category is not None and category not in valid:
            raise ValueError(
                f"set_unfindable_category: invalid category {category!r}")
        self.set_unfindable_category_calls.append(
            (request_id, category, categorised_at),
        )
        row = self._requests.get(request_id)
        if row is None:
            return
        if row.get("status") != "wanted":
            return
        row["unfindable_category"] = category
        row["unfindable_categorised_at"] = categorised_at
        row["updated_at"] = categorised_at

    def get_unfindable_search_log_signal(
        self,
        request_id: int,
        *,
        window_days: int,
        matcher_score_threshold: float,
    ) -> Any:
        """Mirror PipelineDB.get_unfindable_search_log_signal.

        Walks ``self.search_logs`` once, applies the same window + filters
        the production SQL applies, and returns the aggregated struct.
        """
        from lib.unfindable_detection_service import UnfindableSearchLogSignal

        cutoff = _utcnow() - timedelta(days=int(window_days))
        cycles: dict[int, int] = {}  # plan_cycle_snapshot -> found count
        wrong_pressing_hits = 0
        for entry in self.search_logs:
            if entry.request_id != request_id:
                continue
            if entry.attempt_consumed is not True:
                continue
            if entry.created_at <= cutoff:
                continue
            cycle = entry.plan_cycle_snapshot
            if cycle is not None:
                found_inc = 1 if entry.outcome == "found" else 0
                cycles[int(cycle)] = cycles.get(int(cycle), 0) + found_inc
            if (
                entry.rejection_reason == "strict_count_mismatch"
                and entry.matcher_score_top1 is not None
                and entry.matcher_score_top1 >= float(matcher_score_threshold)
            ):
                wrong_pressing_hits += 1
        zero_find_cycles = sum(1 for v in cycles.values() if v == 0)
        return UnfindableSearchLogSignal(
            zero_find_cycles=zero_find_cycles,
            wrong_pressing_hits=wrong_pressing_hits,
        )

    # --- youtube_album_mappings (migration 034) ---

    def seed_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[dict[str, Any]],
    ) -> None:
        """Populate the cache for a (release_group_identifier, source) pair.

        Test helper — bypasses the upsert path so tests can pre-seed state
        without exercising the replace semantics under test.
        """
        self._youtube_album_mappings[
            (release_group_identifier, source)
        ] = [copy.deepcopy(r) for r in rows]

    def get_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
    ) -> Optional[list[dict[str, Any]]]:
        """Return all rows for the pair, ordered by ``yt_browse_id`` ASC.

        Returns ``None`` when the pair has never been resolved, and an
        empty list when it has been resolved to an empty matrix. The
        distinction matters: ``[]`` means "we checked and found nothing"
        (cache HIT — don't re-poll YT), while ``None`` means "we have
        no record" (cache MISS — go ask YT).

        Mirrors the real PipelineDB contract; the resolver gate is
        ``if not refresh and cached_rows is not None``.
        """
        if (release_group_identifier, source) not in self._youtube_album_mappings:
            return None
        rows = self._youtube_album_mappings[
            (release_group_identifier, source)
        ]
        return sorted(
            (copy.deepcopy(r) for r in rows),
            key=lambda r: r["yt_browse_id"],
        )

    def find_youtube_album_mapping_for_release(
        self,
        *,
        source: str,
        release_id: str,
        browse_id: str,
    ) -> Optional[dict[str, Any]]:
        for (rg_id, row_source), rows in self._youtube_album_mappings.items():
            if row_source != source:
                continue
            for row in rows:
                if str(row.get("yt_browse_id") or "") != browse_id:
                    continue
                distances = row.get("distances") or []
                if not any(
                    isinstance(entry, dict)
                    and str(entry.get("mbid") or "") == str(release_id)
                    for entry in distances
                ):
                    continue
                out = copy.deepcopy(row)
                out.setdefault("release_group_identifier", rg_id)
                out.setdefault("source", row_source)
                return out
        return None

    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None:
        """Atomically replace the matrix for ``(release_group_identifier, source)``.

        Partial updates are not supported — refresh always replaces. The
        real implementation wraps DELETE + INSERTs in a single transaction;
        the fake just overwrites the dict slot, which is atomic in the
        single-threaded test context.

        Converts each ``PersistedYoutubeRow`` to the stored read-shape dict
        via ``msgspec.to_builtins`` and stamps ``id``,
        ``release_group_identifier``, ``source``, and ``resolved_at`` onto
        each stored row. Production's SELECT projection
        (``PipelineDB.get_youtube_album_mapping``) always includes these
        DB-assigned columns (``id BIGSERIAL PRIMARY KEY``, ``resolved_at
        TIMESTAMPTZ NOT NULL DEFAULT NOW()`` — migration 034) even though
        callers never pass them into ``rows``. #523's read-projection
        parity gate surfaced this: the fake previously echoed the input
        dict verbatim, four keys short of what production's read returns.
        """
        stored: list[dict[str, Any]] = []
        for row in rows:
            self._next_youtube_mapping_id += 1
            stored_row: dict[str, Any] = msgspec.to_builtins(row)
            stored_row["id"] = self._next_youtube_mapping_id
            stored_row["release_group_identifier"] = release_group_identifier
            stored_row["source"] = source
            stored_row.setdefault("yt_audio_playlist_id", None)
            stored_row.setdefault("yt_year", None)
            stored_row.setdefault("album_title", None)
            stored_row.setdefault("album_artist", None)
            stored_row["resolved_at"] = _utcnow()
            stored.append(stored_row)
        self._youtube_album_mappings[
            (release_group_identifier, source)
        ] = stored

    # --- Session lifecycle ---

    def close(self) -> None:
        """Record that the fake connection was closed. No-op otherwise."""
        self.closed = True
        self.close_calls += 1

    # --- album_requests write + query ---

    def add_request(self, artist_name: str, album_title: str, source: str,
                    mb_release_id: str | None = None,
                    mb_release_group_id: str | None = None,
                    mb_artist_id: str | None = None,
                    discogs_release_id: str | None = None,
                    year: int | None = None, country: str | None = None,
                    format: str | None = None,
                    source_path: str | None = None,
                    reasoning: str | None = None,
                    status: str = "wanted",
                    release_group_year: int | None = None,
                    is_va_compilation: bool = False) -> int:
        """Insert an album_requests row.

        Seeds the full ``album_requests`` column set (matching
        ``make_request_row`` in ``tests/helpers.py``) so fake-backed
        tests that then read DB-defaulted fields like ``beets_distance``
        or ``*_attempts`` see the same NULL/0 defaults production
        callers get from PostgreSQL. Codex R7.
        """
        self._assert_mb_release_id_unique(mb_release_id)
        self._next_request_id += 1
        rid = self._next_request_id
        now = _utcnow()
        self._requests[rid] = {
            "id": rid,
            "mb_release_id": mb_release_id,
            "mb_release_group_id": mb_release_group_id,
            "mb_artist_id": mb_artist_id,
            "discogs_release_id": discogs_release_id,
            "artist_name": artist_name,
            "album_title": album_title,
            "year": year,
            # U3 / R9 — release-group's first-release year. Populated by
            # the deploy-time backfill or U4's enqueue path; nullable.
            "release_group_year": release_group_year,
            # Migration 028 / U4 — VA detection flag, set at enqueue or by
            # the U3 backfill. NOT NULL DEFAULT FALSE matches the schema.
            "is_va_compilation": bool(is_va_compilation),
            "country": country,
            "format": format,
            "source": source,
            "source_path": source_path,
            "reasoning": reasoning,
            "status": status,
            # Migration 032 — resolver-populated catalog number; migration
            # 001 — download-time final container format. Neither is part
            # of ``AddRequestInput`` (production's INSERT column list), so
            # a freshly-added real row has both NULL until a later UPDATE
            # populates them. #523 read-projection parity (get_wanted_
            # searchable's ``ar.*``) surfaced these as missing from the
            # fake's row shape.
            "catalog_number": None,
            "final_format": None,
            "search_attempts": 0,
            "download_attempts": 0,
            "validation_attempts": 0,
            "last_attempt_at": None,
            "next_retry_after": None,
            "beets_distance": None,
            "beets_scenario": None,
            "imported_path": None,
            "search_filetype_override": None,
            "target_format": None,
            "min_bitrate": None,
            "prev_min_bitrate": None,
            "last_download_spectral_bitrate": None,
            "last_download_spectral_grade": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "current_lossless_source_v0_probe_min_bitrate": None,
            "current_lossless_source_v0_probe_avg_bitrate": None,
            "current_lossless_source_v0_probe_median_bitrate": None,
            "active_download_state": None,
            # U1 persisted-search-plans cursor fields.
            "active_plan_id": None,
            "next_plan_ordinal": 0,
            "plan_cycle_count": 0,
            # Migration 028 / U12 — failure_class is materialised at
            # plan-wrap; NULL until the first cycle completes.
            "failure_class": None,
            # Migration 028 / U13 — unfindable detection state. All
            # nullable; the daily detection job populates the four-
            # category taxonomy via the dedicated systemd unit.
            "unfindable_category": None,
            "unfindable_categorised_at": None,
            "last_artist_probe_at": None,
            "last_artist_probe_match_count": None,
            # Migration 028 / U14 — long-tail-rescue audit columns.
            "rescued_at": None,
            "prior_unfindable_category": None,
            # Migration 021 addressing FK.
            "current_evidence_id": None,
            # Migration 023 — supersede lineage.
            "replaces_request_id": None,
            "created_at": now,
            "updated_at": now,
        }
        return rid

    def supersede_request_mbid(
        self,
        old_request_id: int,
        *,
        new_mb_release_id: str,
        new_mb_release_group_id: str | None,
        new_mb_artist_id: str | None,
        new_artist_name: str,
        new_album_title: str,
        new_year: int | None,
        new_country: str | None,
        new_tracks: list[dict[str, Any]],
        new_discogs_release_id: str | None = None,
    ) -> int:
        """In-memory mirror of ``PipelineDB.supersede_request_mbid``.

        Raises ``MbidCollisionError`` when ``new_mb_release_id`` already
        exists in any row; ``SupersedeRaceError`` when the old row is
        missing or already ``status='replaced'``.
        """
        from lib.pipeline_db import (
            MbidCollisionError,
            SupersedeRaceError,
        )

        old_row = self._requests.get(old_request_id)
        if old_row is None:
            raise SupersedeRaceError(
                f"old request {old_request_id} not found"
            )
        if old_row.get("status") == "replaced":
            raise SupersedeRaceError(
                f"old request {old_request_id} already replaced"
            )
        # Collision check.
        for r in self._requests.values():
            if r.get("mb_release_id") == new_mb_release_id:
                raise MbidCollisionError(
                    f"target MBID {new_mb_release_id} already exists"
                )

        now = _utcnow()
        old_source = old_row.get("source", "request")
        # Flip old row: status=replaced + clear imported_path. Nothing
        # else is mutated — characteristic fields stay frozen.
        old_row["status"] = "replaced"
        old_row["imported_path"] = None
        old_row["updated_at"] = now

        # Insert new row via add_request to inherit the seeded defaults,
        # then patch the supersede-only fields.
        new_id = self.add_request(
            artist_name=new_artist_name,
            album_title=new_album_title,
            source=old_source,
            mb_release_id=new_mb_release_id,
            mb_release_group_id=new_mb_release_group_id,
            mb_artist_id=new_mb_artist_id,
            discogs_release_id=new_discogs_release_id,
            year=new_year,
            country=new_country,
            status="wanted",
        )
        self._requests[new_id]["replaces_request_id"] = old_request_id

        # Insert tracks.
        self._tracks[new_id] = [
            {
                "disc_number": t.get("disc_number", 1),
                "track_number": t["track_number"],
                "title": t["title"],
                "length_seconds": t.get("length_seconds"),
                "track_artist": t.get("track_artist"),
            }
            for t in new_tracks
        ]
        return new_id

    def get_request_by_replaces_request_id(
        self, replaced_id: int
    ) -> dict[str, Any] | None:
        """Reverse-lookup the descendant row of ``replaced_id``."""
        for row in self._requests.values():
            if row.get("replaces_request_id") == replaced_id:
                return copy.deepcopy(row)
        return None

    def get_oldest_request_chain_created_at(
        self, request_id: int
    ) -> datetime | None:
        """Oldest ``created_at`` across the replace chain, walking
        ``replaces_request_id`` back through superseded ancestors —
        mirrors the recursive CTE in ``_RequestsMixin``."""
        oldest: datetime | None = None
        seen: set[int] = set()
        cursor: int | None = request_id
        while cursor is not None and cursor not in seen:
            seen.add(cursor)
            row = self._requests.get(cursor)
            if row is None:
                break
            created = row.get("created_at")
            if created is not None:
                created = _as_datetime(created)
                if oldest is None or created < oldest:
                    oldest = created
            cursor = row.get("replaces_request_id")
        return oldest

    def list_requests_in_release_group(
        self,
        rg_id: str,
        *,
        exclude_replaced: bool = True,
        exclude_request_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """List rows in the same MB release group (newest id first)."""
        out: list[dict[str, Any]] = []
        for row in self._requests.values():
            if row.get("mb_release_group_id") != rg_id:
                continue
            if exclude_replaced and row.get("status") == "replaced":
                continue
            if exclude_request_id is not None and row.get("id") == exclude_request_id:
                continue
            out.append(copy.deepcopy(row))
        out.sort(key=lambda r: r["id"], reverse=True)
        return out

    def list_active_release_group_ids(self) -> set[str]:
        """Distinct set of RG ids across non-replaced rows."""
        return {
            row["mb_release_group_id"]
            for row in self._requests.values()
            if row.get("status") != "replaced"
            and row.get("mb_release_group_id") is not None
        }

    def list_non_replaced_requests(self) -> list[dict[str, Any]]:
        """Return active request rows ordered like PipelineDB."""
        rows = [
            r for r in self._requests.values()
            if r.get("status") != "replaced"
        ]
        rows.sort(key=lambda r: int(r["id"]))
        return [copy.deepcopy(r) for r in rows]

    def delete_request(self, request_id: int) -> None:
        """Delete a request and cascade to child tables.

        Real ``album_requests`` has ``ON DELETE CASCADE`` foreign keys
        from ``album_tracks``, ``download_log``, ``search_log``, and
        ``source_denylist`` (see ``migrations/001_initial.sql``). Mirror
        that here so fake-backed tests cannot observe an impossible
        post-delete state where child rows survive their parent.
        """
        self._requests.pop(request_id, None)
        self._tracks.pop(request_id, None)
        self.download_logs = [
            e for e in self.download_logs if e.request_id != request_id]
        self.search_logs = [
            e for e in self.search_logs if e.request_id != request_id]
        self.denylist = [
            e for e in self.denylist if e.request_id != request_id]
        # Migration 021: evidence is content-addressed; deleting a request
        # no longer cascades into evidence rows. Addressing FKs on
        # album_requests / download_log / import_jobs were nulled by the
        # earlier reassignments above.
        # U1: cascade plans + items with the request, mirroring the real
        # ON DELETE CASCADE FKs from migration 014.
        plan_ids_to_drop = [
            pid for pid, plan in self.search_plans.items()
            if plan.request_id == request_id
        ]
        for pid in plan_ids_to_drop:
            self.search_plans.pop(pid, None)
        self.search_plan_items = {
            iid: item for iid, item in self.search_plan_items.items()
            if item.plan_id not in plan_ids_to_drop
        }

    def get_wanted(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return wanted requests past their retry gate.

        Production randomizes the diagnostic result.  The fake keeps insertion
        order for deterministic tests, but does not apply attempt-count
        priority.
        """
        now = _utcnow()
        eligible = [
            r for r in self._requests.values()
            if r.get("status") == "wanted"
            and (r.get("next_retry_after") is None
                 or r["next_retry_after"] <= now)
        ]
        if limit is not None:
            eligible = eligible[:int(limit)]
        return [copy.deepcopy(r) for r in eligible]

    def get_download_log_counts(self) -> DownloadLogCounts:
        """Mirror of ``PipelineDB.get_download_log_counts`` — computed
        from the fake's real ``download_logs``/``search_logs`` state,
        never queued (#445 item 2). Parity with the production SQL is
        pinned by ``tests/test_pipeline_db.py::TestGetDownloadLogCounts``.
        """
        now = _utcnow()
        total = len(self.download_logs)
        imported = sum(
            1 for e in self.download_logs
            if e.outcome in ("success", "force_import"))
        found_24h = sum(
            1 for e in self.search_logs
            if e.outcome == "found"
            and self._as_utc(e.created_at) >= now - timedelta(hours=24))
        found_6h = sum(
            1 for e in self.search_logs
            if e.outcome == "found"
            and self._as_utc(e.created_at) >= now - timedelta(hours=6))
        return DownloadLogCounts(
            total=total, imported=imported,
            matches_24h=found_24h, matches_6h=found_6h)

    def get_pipeline_overlay(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Mirror of ``PipelineDB.get_pipeline_overlay`` — projects the
        overlay fields straight from seeded request rows (#445 item 2).
        Parity pinned by ``TestGetPipelineOverlay``."""
        wanted = {str(m) for m in mbids}
        out: dict[str, dict[str, Any]] = {}
        for r in self._requests.values():
            raw = r.get("mb_release_id")
            # Production's column is TEXT — compare and key by the str
            # form (a None mbid must never stringify into a match).
            if raw is None:
                continue
            mbid = str(raw)
            if mbid in wanted:
                evidence = None
                evidence_id = r.get("current_evidence_id")
                if evidence_id is not None:
                    evidence = self.load_album_quality_evidence_by_id(
                        evidence_id)
                verified = bool(
                    evidence is not None
                    and evidence.verified_lossless_proof is not None
                )
                provisional = bool(
                    evidence is not None
                    and not verified
                    and evidence.v0_metric is not None
                    and evidence.v0_metric.subject == "source"
                )
                out[mbid] = {
                    "id": r["id"],
                    "status": r.get("status"),
                    "search_filetype_override":
                        r.get("search_filetype_override"),
                    "target_format": r.get("target_format"),
                    "min_bitrate": r.get("min_bitrate"),
                    "verified_lossless": verified,
                    "provisional_lossless": provisional,
                }
        return out

    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None,
                ) -> list[dict[str, object]]:
        imported = {"success", "force_import"}
        rejected = {"rejected", "failed", "timeout", "measurement_failed"}
        rows: list[dict[str, object]] = []
        # Newest-first to match the real ORDER BY dl.created_at DESC.
        for entry in reversed(self.download_logs):
            if outcome_filter == "imported" and entry.outcome not in imported:
                continue
            if outcome_filter == "rejected" and entry.outcome not in rejected:
                continue
            req = self._requests.get(entry.request_id, {})
            # Real SQL is ``SELECT dl.*, ar.album_title, …`` — every
            # download_log column must appear, including the auxiliary
            # fields ``log_download`` parks in ``entry.extra``
            # (bitrate, actual_filetype, spectral_grade, final_format,
            # etc.). Dropping them here would silently mis-classify rows
            # in callers that feed ``get_log`` into LogEntry.from_row.
            joined: dict[str, object] = self._download_log_to_dict(entry)
            joined.update({
                # Joined request columns.
                "album_title": req.get("album_title"),
                "artist_name": req.get("artist_name"),
                "mb_release_id": req.get("mb_release_id"),
                "year": req.get("year"),
                "country": req.get("country"),
                "request_status": req.get("status"),
                "request_min_bitrate": req.get("min_bitrate"),
                "prev_min_bitrate": req.get("prev_min_bitrate"),
                "search_filetype_override": req.get(
                    "search_filetype_override"),
                "request_source": req.get("source"),
            })
            current_evidence_id = req.get("current_evidence_id")
            current_evidence = (
                self._evidence_by_id.get(int(current_evidence_id))
                if current_evidence_id is not None else None
            )
            current_measurement = (
                current_evidence.measurement
                if current_evidence is not None else None
            )
            current_v0 = (
                current_evidence.v0_metric
                if current_evidence is not None else None
            )
            joined.update({
                "_current_evidence_id": (
                    current_evidence.id if current_evidence is not None else None
                ),
                "_current_evidence_is_pre_attempt": (
                    current_evidence.measured_at <= entry.created_at
                    if current_evidence is not None else None
                ),
                "_current_evidence_format": (
                    current_measurement.format
                    if current_measurement is not None else None
                ),
                "_current_evidence_min_bitrate": (
                    current_measurement.min_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_avg_bitrate": (
                    current_measurement.avg_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_median_bitrate": (
                    current_measurement.median_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_spectral_grade": (
                    current_measurement.spectral_grade
                    if current_measurement is not None else None
                ),
                "_current_evidence_spectral_bitrate": (
                    current_measurement.spectral_bitrate_kbps
                    if current_measurement is not None else None
                ),
                "_current_evidence_v0_probe_kind": (
                    current_v0.subject if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_min_bitrate": (
                    current_v0.min_bitrate_kbps if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_avg_bitrate": (
                    current_v0.avg_bitrate_kbps if current_v0 is not None else None
                ),
                "_current_evidence_v0_probe_median_bitrate": (
                    current_v0.median_bitrate_kbps if current_v0 is not None else None
                ),
            })
            rows.append(joined)
            if len(rows) >= limit:
                break
        return rows

    def get_linked_import_logs(
        self,
        source_log_ids: list[int],
    ) -> list[dict[str, object]]:
        wanted = {int(log_id) for log_id in source_log_ids}
        return [
            self._download_log_to_dict(entry)
            for entry in reversed(self.download_logs)
            if entry.source_download_log_id in wanted
            and entry.outcome in ("success", "force_import", "manual_import")
        ]

    def get_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[dict[str, Any]]:
        if newest_first:
            rows = sorted(
                (r for r in self._requests.values()
                 if r.get("status") == status),
                key=lambda r: _as_datetime(r.get("updated_at")),
                reverse=True)
        else:
            rows = sorted(
                (r for r in self._requests.values()
                 if r.get("status") == status),
                key=lambda r: _as_datetime(r.get("created_at")))
        if limit is not None:
            rows = rows[:int(limit)]
        return [copy.deepcopy(r) for r in rows]

    def search_requests(
        self, query: str, *, limit: int = 200, status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Mirror ``PipelineDB.search_requests``: case-insensitive
        substring over artist/album, optionally narrowed to one status."""
        q = (query or "").strip().lower()
        if not q:
            return []
        rows = [
            r for r in self._requests.values()
            if (q in str(r.get("artist_name") or "").lower()
                or q in str(r.get("album_title") or "").lower())
            and (status is None or r.get("status") == status)
        ]
        rows.sort(key=lambda r: (
            str(r.get("artist_name") or ""),
            r.get("year") is None,
            int(str(r.get("year") or 0)),
            int(str(r["id"])),
        ))
        return [copy.deepcopy(r) for r in rows[:int(limit)]]

    def _has_youtube_running(self, request_id: int) -> bool:
        """Mirror of the ``_LONG_TAIL_SELECT`` ``youtube_running`` EXISTS.

        A request has an in-flight rescue iff a ``download_log`` row with
        ``source='youtube' AND outcome='youtube_running'`` exists for it.
        """
        return any(
            entry.source == "youtube"
            and entry.outcome == "youtube_running"
            and entry.request_id == request_id
            for entry in self.download_logs
        )

    def _long_tail_projection(self, row: dict[str, Any]) -> dict[str, Any]:
        """Project a request row to the long-tail cohort SELECT shape.

        Mirrors ``PipelineDB._LONG_TAIL_SELECT``'s narrow column list +
        the ``in_flight_rescue`` stamp so tests can't rely on a column
        the production query doesn't return.
        """
        keys = (
            "id", "artist_name", "album_title", "year", "status", "source",
            "mb_release_id", "mb_release_group_id", "discogs_release_id",
            "target_format", "min_bitrate", "search_filetype_override",
            "unfindable_category", "current_spectral_grade",
            "current_spectral_bitrate",
        )
        out: dict[str, Any] = {k: row.get(k) for k in keys}
        # track_count mirrors the production COUNT(*) over album_tracks.
        out["track_count"] = len(self._tracks.get(int(row["id"]), []))
        out["in_flight_rescue"] = self._has_youtube_running(int(row["id"]))
        return out

    def get_long_tail_cohort(self) -> list[dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.get_long_tail_cohort``.

        Returns every ``wanted`` request projected to the cohort SELECT
        shape, id ASC, each stamped with ``in_flight_rescue``. Counts as
        ONE query for the N+1 guard.
        """
        self.query_counts["get_long_tail_cohort"] = (
            self.query_counts.get("get_long_tail_cohort", 0) + 1
        )
        rows = sorted(
            (r for r in self._requests.values()
             if r.get("status") == "wanted"),
            key=lambda r: int(r["id"]),
        )
        return [self._long_tail_projection(r) for r in rows]

    def get_long_tail_request(
        self, request_id: int,
    ) -> dict[str, Any] | None:
        """In-memory mirror of ``PipelineDB.get_long_tail_request``.

        Single-id variant — returns ``None`` when the row is missing or
        no longer ``wanted``.
        """
        self.query_counts["get_long_tail_request"] = (
            self.query_counts.get("get_long_tail_request", 0) + 1
        )
        row = self._requests.get(int(request_id))
        if row is None or row.get("status") != "wanted":
            return None
        return self._long_tail_projection(row)

    def count_by_status(self) -> dict[str | None, int]:
        counts: dict[str | None, int] = {}
        for r in self._requests.values():
            status = r.get("status")
            counts[status] = counts.get(status, 0) + 1
        return counts

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, Any]]:
        needle = artist_name.lower()

        def _legacy_name_match(row: dict[str, Any]) -> bool:
            artist = str(row.get("artist_name") or "").lower()
            artist_id = row.get("mb_artist_id")
            artist_id_str = str(artist_id or "")
            return (
                needle in artist
                and (
                    artist_id is None
                    or artist_id_str == ""
                    or "-" not in artist_id_str
                )
            )

        rows: list[dict[str, Any]] = []
        for row in self._requests.values():
            if mb_artist_id:
                if row.get("mb_artist_id") == mb_artist_id or _legacy_name_match(row):
                    rows.append(copy.deepcopy(row))
            else:
                if needle in str(row.get("artist_name") or "").lower():
                    rows.append(copy.deepcopy(row))

        def _sort_key(row: dict[str, Any]) -> tuple[bool, int, str]:
            year = row.get("year")
            year_num = int(year) if isinstance(year, int) else 0
            title = str(row.get("album_title") or "")
            return (year is not None, year_num, title)

        rows.sort(key=_sort_key)
        return rows

    # --- Track management ---

    def set_tracks(self, request_id: int,
                   tracks: list[dict[str, Any]]) -> None:
        row = self._requests.get(request_id)
        if row is None:
            raise ValueError(f"request {request_id} not found")
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        self._tracks[request_id] = [
            {
                "disc_number": t.get("disc_number", 1),
                "track_number": t["track_number"],
                "title": t["title"],
                "length_seconds": t.get("length_seconds"),
                # PR2 U2 / R13: per-track artist from upstream payload.
                # Real PipelineDB stores this in album_tracks.track_artist
                # (migration 029). NULL is the legitimate default — the
                # resolver fills it later via ``update_track_artists``.
                "track_artist": t.get("track_artist"),
            }
            for t in tracks
        ]

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]:
        rows = list(self._tracks.get(request_id, []))
        rows.sort(key=lambda t: (t["disc_number"], t["track_number"]))
        return [copy.deepcopy(t) for t in rows]

    def update_track_artists(
        self, request_id: int,
        track_artists: list[str | None],
        *,
        expected_status: str | None = None,
    ) -> bool:
        """Mirror of ``PipelineDB.update_track_artists`` — apply per-track
        artists in (disc, track) order. Length mismatches are tolerated
        (fewer keeps existing, more drops extras) — same shape as real.
        """
        request = self._requests.get(request_id)
        if (
            request is None
            or request.get("status") == "replaced"
            or (
                expected_status is not None
                and request.get("status") != expected_status
            )
        ):
            return False
        if not track_artists:
            return True
        rows = self._tracks.get(request_id, [])
        if not rows:
            return True
        rows.sort(key=lambda t: (t["disc_number"], t["track_number"]))
        for row, artist in zip(rows, track_artists):
            row["track_artist"] = artist
        return True

    def get_track_counts(self,
                         request_ids: list[int]) -> dict[int, int]:
        return {
            rid: len(self._tracks[rid])
            for rid in request_ids
            if rid in self._tracks and self._tracks[rid]
        }

    # --- Download history queries ---

    def get_download_log_entry(self,
                               log_id: int) -> dict[str, Any] | None:
        for entry in self.download_logs:
            if entry.id == log_id:
                return self._download_log_to_dict(entry)
        return None

    def get_download_history(self,
                             request_id: int) -> list[dict[str, Any]]:
        return [
            self._download_log_to_dict(e)
            for e in reversed(self.download_logs)
            if e.request_id == request_id
        ]

    def get_download_history_batch(
        self, request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]:
        wanted = set(request_ids)
        result: dict[int, list[dict[str, Any]]] = {}
        for entry in reversed(self.download_logs):
            if entry.request_id not in wanted:
                continue
            result.setdefault(entry.request_id, []).append(
                self._download_log_to_dict(entry))
        return result

    def get_latest_download_summaries(
        self, request_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """Mirror ``PipelineDB.get_latest_download_summaries``: newest
        row + history count per request (#426)."""
        return {
            rid: {"latest": history[0], "count": len(history)}
            for rid, history in
            self.get_download_history_batch(request_ids).items()
        }

    # --- Pipeline dashboard telemetry ---

    def record_cycle_metrics(
        self,
        *,
        cycle_total_s: float,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        browse_time_s: float = 0.0,
        match_time_s: float = 0.0,
        search_time_s: float = 0.0,
        cache_pos_hits: int = 0,
        cache_neg_hits: int = 0,
        cache_misses: int = 0,
        cache_errors: int = 0,
        cache_fuse_tripped: int = 0,
        cache_write_errors: int = 0,
        peers_browsed: int = 0,
        peers_browsed_lazy: int = 0,
        fanout_waves: int = 0,
        cycle_searches_watchdog_killed: int = 0,
        find_download_queued: int = 0,
        find_download_completed: int = 0,
        find_download_drain_time_s: float = 0.0,
        wanted_total: int | None = None,
    ) -> int:
        wanted_snapshot = (
            self._current_wanted_total() if wanted_total is None
            else max(0, int(wanted_total))
        )
        row = {
            "id": len(self.cycle_metrics) + 1,
            "started_at": started_at,
            "created_at": completed_at or _utcnow(),
            "cycle_total_s": cycle_total_s,
            "browse_time_s": browse_time_s,
            "match_time_s": match_time_s,
            "search_time_s": search_time_s,
            "cache_pos_hits": cache_pos_hits,
            "cache_neg_hits": cache_neg_hits,
            "cache_misses": cache_misses,
            "cache_errors": cache_errors,
            "cache_fuse_tripped": cache_fuse_tripped,
            "cache_write_errors": cache_write_errors,
            "peers_browsed": peers_browsed,
            "peers_browsed_lazy": peers_browsed_lazy,
            "fanout_waves": fanout_waves,
            "cycle_searches_watchdog_killed": cycle_searches_watchdog_killed,
            "find_download_queued": find_download_queued,
            "find_download_completed": find_download_completed,
            "find_download_drain_time_s": find_download_drain_time_s,
            "wanted_total": wanted_snapshot,
        }
        self.cycle_metrics.append(row)
        return int(row["id"])

    def _current_wanted_total(self) -> int:
        return sum(1 for req in self._requests.values()
                   if req.get("status") in ("wanted", "downloading"))

    def _dashboard_wanted_trend(self, current_wanted: int) -> dict[str, Any]:
        now = _utcnow()
        samples: list[tuple[datetime, int]] = []
        for row in sorted(self.cycle_metrics, key=lambda r: r["created_at"]):
            if row.get("wanted_total") is None:
                continue
            created_at = row["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            else:
                created_at = created_at.astimezone(timezone.utc)
            if created_at >= now - timedelta(days=7):
                samples.append((created_at, int(row["wanted_total"])))

        def _window(label: str, hours: int) -> dict[str, Any]:
            window_start = now - timedelta(hours=hours)
            window_samples = [
                (at, wanted) for at, wanted in samples
                if at >= window_start
            ]
            if not window_samples:
                return {
                    "label": label,
                    "hours": hours,
                    "sample_count": 0,
                    "start_sample_at": None,
                    "end_sample_at": now.isoformat(),
                    "start_wanted": None,
                    "end_wanted": current_wanted,
                    "delta": None,
                    "delta_per_hour": None,
                    "drain_per_hour": None,
                    "eta_hours": None,
                    "trend": "unknown",
                }
            start_at, start_wanted = window_samples[0]
            elapsed_hours = (now - start_at).total_seconds() / 3600
            delta = current_wanted - start_wanted
            if elapsed_hours <= 0:
                delta_per_hour = None
                drain_per_hour = None
                eta_hours = None
                trend = "unknown"
            else:
                delta_per_hour = delta / elapsed_hours
                drain_per_hour = max(-delta_per_hour, 0.0)
                eta_hours = (
                    current_wanted / drain_per_hour
                    if drain_per_hour > 0 and current_wanted > 0
                    else None
                )
                trend = "down" if delta < 0 else "up" if delta > 0 else "flat"
            return {
                "label": label,
                "hours": hours,
                "sample_count": len(window_samples),
                "start_sample_at": start_at.isoformat(),
                "end_sample_at": now.isoformat(),
                "start_wanted": start_wanted,
                "end_wanted": current_wanted,
                "delta": delta,
                "delta_per_hour": delta_per_hour,
                "drain_per_hour": drain_per_hour,
                "eta_hours": eta_hours,
                "trend": trend,
            }

        return {
            "current_wanted": current_wanted,
            "latest_sample_at": samples[-1][0].isoformat() if samples else None,
            "series_24h": [
                {"sampled_at": at.isoformat(), "wanted_total": wanted}
                for at, wanted in samples
                if at >= now - timedelta(hours=24)
            ] + [{
                "sampled_at": now.isoformat(),
                "wanted_total": current_wanted,
                "synthetic": True,
            }],
            "windows": [
                _window("6h", 6),
                _window("24h", 24),
                _window("7d", 24 * 7),
            ],
        }

    def record_peer_observations(
        self,
        usernames: Iterable[str],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        from lib.pipeline_db import _peer_hash

        observed = observed_at or _utcnow()
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        unique = sorted({str(u) for u in usernames if u})
        new_count = 0
        for username in unique:
            username_hash = _peer_hash(username)
            row = self.peer_observations.get(username_hash)
            if row is None:
                self.peer_observations[username_hash] = {
                    "username_hash": username_hash,
                    "first_seen_at": observed,
                    "last_seen_at": observed,
                }
                new_count += 1
            else:
                row["last_seen_at"] = max(row["last_seen_at"], observed)
        return new_count

    def get_peer_metrics(self, days: int = 14) -> dict[str, Any]:
        """Mirror ``PipelineDB.get_peer_metrics``: live totals plus a
        Perth-local per-day growth curve with cumulative ``total_peers``."""
        clamped_days = max(1, min(int(days), 90))
        rows = list(self.peer_observations.values())

        today_perth = _utcnow().astimezone(_PERTH_TZ).date()
        window_start = today_perth - timedelta(days=clamped_days - 1)

        new_by_day: dict[date, int] = {}
        for row in rows:
            ts = row["first_seen_at"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            day = ts.astimezone(_PERTH_TZ).date()
            new_by_day[day] = new_by_day.get(day, 0) + 1

        day_dicts: list[dict[str, Any]] = []
        cursor = today_perth
        while cursor >= window_start:
            day_dicts.append({
                "date": cursor.isoformat(),
                "new_peers": new_by_day.get(cursor, 0),
                "total_peers": sum(
                    count for day, count in new_by_day.items()
                    if day <= cursor
                ),
            })
            cursor = cursor - timedelta(days=1)

        now = _utcnow()
        return {
            "days": day_dicts,
            "totals": {
                "known_peers": len(rows),
                "new_24h": sum(
                    1 for row in rows
                    if row["first_seen_at"] >= now - timedelta(hours=24)
                ),
                "seen_24h": sum(
                    1 for row in rows
                    if row["last_seen_at"] >= now - timedelta(hours=24)
                ),
                "tracked_since": (
                    min(row["first_seen_at"] for row in rows).isoformat()
                    if rows
                    else None
                ),
            },
        }

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _dashboard_search_window(
        self, label: str, hours: int, now: datetime,
    ) -> dict[str, Any]:
        cutoff = now - timedelta(hours=hours)
        rows = [e for e in self.search_logs
                if self._as_utc(e.created_at) >= cutoff]
        # Errors bucket mirrors the SQL FILTER exactly: only timeout /
        # error / empty_query count — an unknown outcome counts toward
        # ``searches`` but no bucket.
        outcomes = {
            "found": sum(1 for e in rows if e.outcome == "found"),
            "no_match": sum(1 for e in rows if e.outcome == "no_match"),
            "no_results": sum(
                1 for e in rows if e.outcome == "no_results"),
            "exhausted": sum(1 for e in rows if e.outcome == "exhausted"),
            "errors": sum(
                1 for e in rows
                if e.outcome in ("timeout", "error", "empty_query")),
        }
        elapsed = sorted(
            e.elapsed_s for e in rows if e.elapsed_s is not None)

        def _pct(p: float) -> float | None:
            if not elapsed:
                return None
            return elapsed[min(len(elapsed) - 1, int(len(elapsed) * p))]

        return {
            "label": label,
            "hours": hours,
            "searches": len(rows),
            "distinct_requests": len({e.request_id for e in rows}),
            "searches_per_hour": len(rows) / hours,
            "searches_per_24h": len(rows) / hours * 24,
            "avg_elapsed_s": (sum(elapsed) / len(elapsed)) if elapsed else None,
            "median_elapsed_s": _pct(0.5),
            "p95_elapsed_s": _pct(0.95),
            "max_elapsed_s": elapsed[-1] if elapsed else None,
            "outcomes": outcomes,
            "cursor_wraps": sum(
                1 for e in rows if e.cursor_update_status == "wrapped"),
            "stale_completions": sum(
                1 for e in rows if e.cursor_update_status == "stale"),
            "non_consuming": sum(
                1 for e in rows if e.attempt_consumed is False),
            "cache_attribution_level": "cycle_only",
        }

    def _dashboard_cycle_window(
        self, label: str, hours: int, now: datetime,
    ) -> dict[str, Any]:
        cutoff = now - timedelta(hours=hours)
        rows = [r for r in self.cycle_metrics
                if self._as_utc(r["created_at"]) >= cutoff]
        totals = sorted(float(r["cycle_total_s"]) for r in rows)
        searches = sorted(float(r["search_time_s"]) for r in rows)

        def _pct(values: list[float], p: float) -> float | None:
            if not values:
                return None
            return values[min(len(values) - 1, int(len(values) * p))]

        return {
            "label": label,
            "hours": hours,
            "cycles": len(rows),
            "avg_cycle_s": (sum(totals) / len(totals)) if totals else None,
            "median_cycle_s": _pct(totals, 0.5),
            "p95_cycle_s": _pct(totals, 0.95),
            "max_cycle_s": totals[-1] if totals else None,
            "median_search_s": _pct(searches, 0.5),
            "watchdog_kills": sum(
                int(r["cycle_searches_watchdog_killed"]) for r in rows),
            "find_download_queued": sum(
                int(r["find_download_queued"]) for r in rows),
            "find_download_completed": sum(
                int(r["find_download_completed"]) for r in rows),
            "cache_errors": sum(int(r["cache_errors"]) for r in rows),
            "cache_write_errors": sum(
                int(r["cache_write_errors"]) for r in rows),
            "cache_fuse_tripped": sum(
                int(r["cache_fuse_tripped"]) for r in rows),
            "peers_browsed": sum(int(r["peers_browsed"]) for r in rows),
            "peers_browsed_lazy": sum(
                int(r["peers_browsed_lazy"]) for r in rows),
            "fanout_waves": sum(int(r["fanout_waves"]) for r in rows),
        }

    def _dashboard_cycle_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Mirror ``PipelineDB._serialize_dashboard_cycle_row`` exactly:
        fixed key set, ``cycle_searches_watchdog_killed`` renamed to
        ``watchdog_kills``, cache hit/miss + wanted_total columns NOT
        emitted, timestamps isoformatted."""
        def _iso(value: Any) -> str | None:
            return value.isoformat() if isinstance(value, datetime) else value
        return {
            "id": int(row["id"]),
            "started_at": _iso(row.get("started_at")),
            "created_at": _iso(row.get("created_at")),
            "cycle_total_s": float(row["cycle_total_s"]),
            "browse_time_s": float(row["browse_time_s"]),
            "match_time_s": float(row["match_time_s"]),
            "search_time_s": float(row["search_time_s"]),
            "watchdog_kills": int(row["cycle_searches_watchdog_killed"]),
            "find_download_queued": int(row["find_download_queued"]),
            "find_download_completed": int(row["find_download_completed"]),
            "find_download_drain_time_s": float(
                row["find_download_drain_time_s"]),
            "cache_errors": int(row["cache_errors"]),
            "cache_write_errors": int(row["cache_write_errors"]),
            "cache_fuse_tripped": int(row["cache_fuse_tripped"]),
            "peers_browsed": int(row["peers_browsed"]),
            "peers_browsed_lazy": int(row["peers_browsed_lazy"]),
            "fanout_waves": int(row["fanout_waves"]),
        }

    def _dashboard_coverage(self, now: datetime) -> dict[str, Any]:
        """Mirror the production coverage CTEs: backlog = wanted +
        downloading; suspects = searched-in-24h rows ordered
        (searches_24h DESC, searches_6h DESC, id ASC) LIMIT 12 with
        reset_24h counting the HISTORICAL ``exhausted`` outcome and
        problem_24h restricted to timeout/error/empty_query;
        stale_wanted = ALL backlog rows ordered last_search_at ASC
        NULLS FIRST LIMIT 12 (recently-searched rows included); the
        match-rate series are DENSE generate_series mirrors (24 hourly /
        28 daily zero-filled buckets); matches_* ride the wanted CTE
        cross-join, so an empty backlog reports 0 matches even when
        found rows exist."""
        backlog = {
            int(r["id"]): r for r in self._requests.values()
            if r.get("status") in ("wanted", "downloading")
        }

        # One pass over search_log per request: rollup of windowed
        # outcome counts + last_search_at.
        rollup: dict[int, dict[str, Any]] = {}
        cutoff_24h = now - timedelta(hours=24)
        cutoff_6h = now - timedelta(hours=6)
        for e in self.search_logs:
            at = self._as_utc(e.created_at)
            r = rollup.setdefault(e.request_id, {
                "last_search_at": None, "searches_24h": 0, "searches_6h": 0,
                "found_24h": 0, "no_match_24h": 0, "no_results_24h": 0,
                "reset_24h": 0, "problem_24h": 0,
            })
            if r["last_search_at"] is None or at > r["last_search_at"]:
                r["last_search_at"] = at
            if at >= cutoff_24h:
                r["searches_24h"] += 1
                if e.outcome == "found":
                    r["found_24h"] += 1
                elif e.outcome == "no_match":
                    r["no_match_24h"] += 1
                elif e.outcome == "no_results":
                    r["no_results_24h"] += 1
                elif e.outcome == "exhausted":
                    r["reset_24h"] += 1
                elif e.outcome in ("timeout", "error", "empty_query"):
                    r["problem_24h"] += 1
            if at >= cutoff_6h:
                r["searches_6h"] += 1

        searched_24h = sum(
            1 for rid in backlog
            if rollup.get(rid, {}).get("searches_24h", 0) > 0)
        searched_6h = sum(
            1 for rid in backlog
            if rollup.get(rid, {}).get("searches_6h", 0) > 0)
        active_24h = sum(
            rollup.get(rid, {}).get("searches_24h", 0) for rid in backlog)
        active_6h = sum(
            rollup.get(rid, {}).get("searches_6h", 0) for rid in backlog)

        found_rows = [e for e in self.search_logs if e.outcome == "found"]
        if backlog:
            matches_24h = sum(
                1 for e in found_rows
                if self._as_utc(e.created_at) >= cutoff_24h)
            matches_6h = sum(
                1 for e in found_rows
                if self._as_utc(e.created_at) >= cutoff_6h)
        else:
            # Production's summary SQL cross-joins match_rates against
            # the wanted CTE — zero backlog rows mean the aggregates
            # COALESCE to 0 regardless of found rows.
            matches_24h = 0
            matches_6h = 0

        # Dense bucket mirrors of generate_series + LEFT JOIN.
        hour_anchor = now.replace(minute=0, second=0, microsecond=0)
        hourly_counts: dict[datetime, int] = {}
        day_anchor = now.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_counts: dict[datetime, int] = {}
        for e in found_rows:
            at = self._as_utc(e.created_at)
            hourly_counts[at.replace(minute=0, second=0, microsecond=0)] = (
                hourly_counts.get(
                    at.replace(minute=0, second=0, microsecond=0), 0) + 1)
            daily_counts[at.replace(
                hour=0, minute=0, second=0, microsecond=0)] = (
                daily_counts.get(at.replace(
                    hour=0, minute=0, second=0, microsecond=0), 0) + 1)
        series_24h = []
        for i in range(23, -1, -1):
            bucket = hour_anchor - timedelta(hours=i)
            n = hourly_counts.get(bucket, 0)
            series_24h.append({
                "bucket_start": bucket.isoformat(),
                "matches": n,
                "matches_per_hour": n,
            })
        series_28d = []
        for i in range(27, -1, -1):
            bucket = day_anchor - timedelta(days=i)
            n = daily_counts.get(bucket, 0)
            series_28d.append({
                "bucket_start": bucket.isoformat(),
                "matches": n,
                "matches_per_day": n,
            })

        def _request_row(rid: int) -> dict[str, Any]:
            req = backlog[rid]
            r = rollup.get(rid, {})
            at = r.get("last_search_at")
            return {
                "request_id": rid,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "status": req.get("status"),
                "last_search_at": at.isoformat() if at else None,
                "searches_24h": int(r.get("searches_24h", 0)),
                "searches_6h": int(r.get("searches_6h", 0)),
                "found_24h": int(r.get("found_24h", 0)),
                "no_match_24h": int(r.get("no_match_24h", 0)),
                "no_results_24h": int(r.get("no_results_24h", 0)),
                "reset_24h": int(r.get("reset_24h", 0)),
                "problem_24h": int(r.get("problem_24h", 0)),
            }

        suspects = [
            _request_row(rid)
            for rid in sorted(
                (rid for rid in backlog
                 if rollup.get(rid, {}).get("searches_24h", 0) > 0),
                key=lambda rid: (
                    -rollup[rid]["searches_24h"],
                    -rollup[rid]["searches_6h"],
                    rid,
                ),
            )
        ][:12]

        def _stale_sort_key(rid: int):
            at = rollup.get(rid, {}).get("last_search_at")
            req = backlog[rid]
            created = self._as_utc(_as_datetime(req.get("created_at")))
            # NULLS FIRST: never-searched rows sort before everything.
            return (at is not None, at or created, created, rid)

        stale = []
        for rid in sorted(backlog, key=_stale_sort_key)[:12]:
            row = _request_row(rid)
            at = rollup.get(rid, {}).get("last_search_at")
            row["hours_since_search"] = (
                (now - at).total_seconds() / 3600 if at else None)
            stale.append(row)

        top_10_searches = sum(r["searches_24h"] for r in suspects[:10])
        top_10_share = (top_10_searches / active_24h) if active_24h else 0

        oldest = None
        searched_ats = [
            rollup[rid]["last_search_at"] for rid in backlog
            if rid in rollup and rollup[rid]["last_search_at"] is not None
        ]
        if searched_ats:
            oldest = min(searched_ats).isoformat()

        return {
            "wanted_total": len(backlog),
            "wanted_searched_24h": searched_24h,
            "wanted_searched_6h": searched_6h,
            "wanted_unsearched_24h": max(len(backlog) - searched_24h, 0),
            "wanted_unsearched_6h": max(len(backlog) - searched_6h, 0),
            "wanted_never_searched": sum(
                1 for rid in backlog
                if rollup.get(rid, {}).get("last_search_at") is None),
            "active_wanted_searches_24h": active_24h,
            "active_wanted_searches_6h": active_6h,
            "oldest_last_search_at": oldest,
            "matches_24h": matches_24h,
            "matches_6h": matches_6h,
            "matches_per_hour_24h": matches_24h / 24,
            "matches_per_hour_6h": matches_6h / 6,
            "match_rate_series_24h": series_24h,
            "match_rate_series_28d": series_28d,
            "wanted_trend": self._dashboard_wanted_trend(
                self._current_wanted_total()),
            "top_10_share_24h": top_10_share,
            "top_loop_suspects": suspects,
            "stale_wanted": stale,
        }

    def _dashboard_heavy_queries(self, now: datetime) -> list[dict[str, Any]]:
        """Mirror ``_dashboard_peer_browse_heavy_queries``: rows with
        (peers_browsed + peers_browsed_lazy) > 0 in the last 24h,
        ordered (peer_dirs DESC, fanout_waves DESC, created_at DESC,
        id DESC), LIMIT 12, with the production serializer's int/float
        coercions (result_count never None)."""
        cutoff = now - timedelta(hours=24)
        rows = [e for e in self.search_logs
                if self._as_utc(e.created_at) >= cutoff
                and (e.peers_browsed + e.peers_browsed_lazy) > 0]
        rows.sort(key=lambda e: (
            -(e.peers_browsed + e.peers_browsed_lazy),
            -e.fanout_waves,
            -self._as_utc(e.created_at).timestamp(),
            -e.id,
        ))
        out: list[dict[str, Any]] = []
        for e in rows[:12]:
            req = self._requests.get(e.request_id, {})
            out.append({
                "search_log_id": e.id,
                "request_id": e.request_id,
                "mb_release_id": req.get("mb_release_id"),
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "status": req.get("status"),
                "created_at": self._as_utc(e.created_at).isoformat(),
                "query": e.query,
                "variant": e.variant,
                "outcome": e.outcome,
                "result_count": int(e.result_count or 0),
                "elapsed_s": float(e.elapsed_s)
                if e.elapsed_s is not None else None,
                "browse_time_s": float(e.browse_time_s or 0.0),
                "match_time_s": float(e.match_time_s or 0.0),
                "peers_browsed": int(e.peers_browsed or 0),
                "peers_browsed_lazy": int(e.peers_browsed_lazy or 0),
                "peer_dirs": int(
                    (e.peers_browsed or 0) + (e.peers_browsed_lazy or 0)),
                "fanout_waves": int(e.fanout_waves or 0),
            })
        return out

    def get_pipeline_dashboard_metrics(
        self,
        *,
        plan_generator_id: str | None = None,
    ) -> dict[str, Any]:
        """Python mirror of the production dashboard read-model.

        Aggregates real seeded telemetry (``search_logs``,
        ``cycle_metrics``, ``peer_observations``, request rows) into the
        same envelope ``PipelineDB.get_pipeline_dashboard_metrics``
        emits, with every timestamp isoformatted exactly like the
        production ``_isoformat_or_none`` boundary (datetimes leaking
        here 500 the dashboard route's json.dumps). Percentiles use a
        simple nearest-rank cut — close enough for contract tests; the
        production SQL is the authority on exact statistics.
        """
        if plan_generator_id is None:
            from lib.search import SEARCH_PLAN_GENERATOR_ID
            plan_generator_id = SEARCH_PLAN_GENERATOR_ID
        now = _utcnow()
        peers = self.get_peer_metrics()
        peers["heavy_queries"] = self._dashboard_heavy_queries(now)
        peers["heavy_query_hours"] = 24
        return {
            "generated_at": now.isoformat(),
            "searches": {
                "windows": [
                    self._dashboard_search_window("24h", 24, now),
                    self._dashboard_search_window("6h", 6, now),
                ],
            },
            "cycles": {
                "windows": [
                    self._dashboard_cycle_window("24h", 24, now),
                    self._dashboard_cycle_window("6h", 6, now),
                ],
                # Production: ORDER BY created_at DESC LIMIT 12 — NOT
                # insertion order (rows seeded with explicit
                # completed_at values must sort by their timestamps).
                "recent": [
                    self._dashboard_cycle_row(r)
                    for r in sorted(
                        self.cycle_metrics,
                        key=lambda row: self._as_utc(row["created_at"]),
                        reverse=True,
                    )[:12]
                ],
                # Production restricts outliers to the last 24 hours.
                "outliers": [
                    self._dashboard_cycle_row(r)
                    for r in sorted(
                        (row for row in self.cycle_metrics
                         if self._as_utc(row["created_at"])
                         >= now - timedelta(hours=24)),
                        key=lambda row: row["cycle_total_s"],
                        reverse=True,
                    )[:8]
                ],
            },
            "coverage": self._dashboard_coverage(now),
            "peers": peers,
            "plan_readiness": self.get_search_plan_readiness(plan_generator_id),
        }

    def get_search_plan_readiness(
        self,
        generator_id: str,
    ) -> dict[str, Any]:
        """Mirror of ``PipelineDB.get_search_plan_readiness`` for tests.

        Walks ``self._requests`` + ``self.search_plans`` to bucket each
        wanted row exactly once. See the live implementation in
        ``lib/pipeline_db.py`` for bucket precedence; both must agree on
        every transition or the dashboard contract breaks silently.
        """
        wanted_total = 0
        wanted_searchable = 0
        wanted_legacy = 0
        wanted_failed_deterministic = 0
        wanted_failed_transient = 0
        wanted_no_plan = 0
        for req in self._requests.values():
            if req.get("status") != "wanted":
                continue
            wanted_total += 1
            active_id = req.get("active_plan_id")
            active_plan = (
                self.search_plans.get(active_id)
                if active_id is not None else None
            )
            if active_plan is not None and active_plan.generator_id == generator_id:
                wanted_searchable += 1
                continue
            if active_plan is not None and active_plan.generator_id != generator_id:
                wanted_legacy += 1
                continue
            # No active plan -- look for failed plans on the current
            # generator id. Deterministic > transient (sticky).
            req_id = req["id"]
            has_det = any(
                p.request_id == req_id
                and p.generator_id == generator_id
                and p.status == "failed_deterministic"
                for p in self.search_plans.values()
            )
            if has_det:
                wanted_failed_deterministic += 1
                continue
            has_trans = any(
                p.request_id == req_id
                and p.generator_id == generator_id
                and p.status == "failed_transient"
                for p in self.search_plans.values()
            )
            if has_trans:
                wanted_failed_transient += 1
                continue
            wanted_no_plan += 1
        return {
            "generator_id": generator_id,
            "wanted_total": wanted_total,
            "wanted_searchable": wanted_searchable,
            "wanted_legacy": wanted_legacy,
            "wanted_failed_deterministic": wanted_failed_deterministic,
            "wanted_failed_transient": wanted_failed_transient,
            "wanted_no_plan": wanted_no_plan,
        }

    def _download_log_to_dict(self,
                              entry: DownloadLogRow) -> dict[str, Any]:
        row: dict[str, Any] = {
            "id": entry.id,
            "request_id": entry.request_id,
            "outcome": entry.outcome,
            "soulseek_username": entry.soulseek_username,
            "filetype": entry.filetype,
            "beets_distance": entry.beets_distance,
            "beets_scenario": entry.beets_scenario,
            "beets_detail": entry.beets_detail,
            "staged_path": entry.staged_path,
            "error_message": entry.error_message,
            "validation_result": entry.validation_result,
            "import_result": entry.import_result,
            # Migration 043 — per-file failure detail audit blob (issue
            # #564 C7).
            "transfer_detail": entry.transfer_detail,
            "created_at": entry.created_at,
            "candidate_evidence_id": entry.candidate_evidence_id,
            "source_download_log_id": entry.source_download_log_id,
            "original_beets_distance": next(
                (
                    origin.beets_distance
                    for origin in self.download_logs
                    if origin.id == entry.source_download_log_id
                ),
                None,
            ),
            # Migration 037 — source discriminator + YT JSONB. Mirrors
            # the production read seam (every consumer sees these two
            # columns whether or not the row originated from YT).
            "source": entry.source,
            "youtube_metadata": copy.deepcopy(entry.youtube_metadata)
            if entry.youtube_metadata is not None else None,
        }
        row.update(entry.extra)
        # Mirror the real LEFT JOIN to album_quality_evidence: prefer
        # evidence-derived measurements over legacy denorm columns when
        # the denorm value is missing. Same semantics as
        # PipelineDB._overlay_evidence_onto_download_log_row.
        ev = self._evidence_by_id.get(entry.candidate_evidence_id) \
            if entry.candidate_evidence_id is not None else None
        if ev is not None:
            ev_m = ev.measurement
            ev_v0 = ev.v0_metric
            source_semantic = ev.lineage_version in (3, 4)
            if source_semantic \
                    and row.get("source_format") is None \
                    and ev_m.format is not None:
                row["source_format"] = ev_m.format
            if source_semantic \
                    and row.get("source_min_bitrate") is None \
                    and ev_m.min_bitrate_kbps is not None:
                row["source_min_bitrate"] = ev_m.min_bitrate_kbps
            if source_semantic \
                    and row.get("source_avg_bitrate") is None \
                    and ev_m.avg_bitrate_kbps is not None:
                row["source_avg_bitrate"] = ev_m.avg_bitrate_kbps
            if source_semantic \
                    and row.get("source_median_bitrate") is None \
                    and ev_m.median_bitrate_kbps is not None:
                row["source_median_bitrate"] = ev_m.median_bitrate_kbps
            if row.get("spectral_grade") is None \
                    and ev_m is not None and ev_m.spectral_grade is not None:
                row["spectral_grade"] = ev_m.spectral_grade
            if row.get("spectral_bitrate") is None \
                    and ev_m is not None \
                    and ev_m.spectral_bitrate_kbps is not None:
                row["spectral_bitrate"] = ev_m.spectral_bitrate_kbps
            if row.get("v0_probe_kind") is None \
                    and ev_v0 is not None:
                row["v0_probe_kind"] = {
                    EVIDENCE_SUBJECT_SOURCE: V0_PROBE_LOSSLESS_SOURCE,
                    EVIDENCE_SUBJECT_INSTALLED: V0_PROBE_NATIVE_LOSSY_RESEARCH,
                }[ev_v0.subject]
            if row.get("v0_probe_min_bitrate") is None \
                    and ev_v0 is not None \
                    and ev_v0.min_bitrate_kbps is not None:
                row["v0_probe_min_bitrate"] = ev_v0.min_bitrate_kbps
            if row.get("v0_probe_avg_bitrate") is None \
                    and ev_v0 is not None \
                    and ev_v0.avg_bitrate_kbps is not None:
                row["v0_probe_avg_bitrate"] = ev_v0.avg_bitrate_kbps
            if row.get("v0_probe_median_bitrate") is None \
                    and ev_v0 is not None \
                    and ev_v0.median_bitrate_kbps is not None:
                row["v0_probe_median_bitrate"] = ev_v0.median_bitrate_kbps
        return row

    # --- Wrong-match review queue ---

    def get_wrong_matches(self) -> list[dict[str, object]]:
        """Rejected downloads whose ``validation_result.failed_path`` is set.

        Mirrors the real ``DISTINCT ON (request_id, failed_path)`` —
        collapse to newest per ``(request_id, failed_path)``, then sort
        newest-first within each request.
        """
        from lib.wrong_match_policy import rejection_scenario_is_wrong_match_candidate

        collapsed: dict[tuple[int, str], DownloadLogRow] = {}
        for entry in self.download_logs:
            if entry.outcome != "rejected":
                continue
            vr = self._validation_result_dict(entry.validation_result)
            failed_path = vr.get("failed_path") if vr else None
            if not failed_path:
                continue
            scenario = vr.get("scenario") if vr else None
            if not rejection_scenario_is_wrong_match_candidate(
                scenario if isinstance(scenario, str) else None
            ):
                continue
            key = (entry.request_id, str(failed_path))
            prev = collapsed.get(key)
            if prev is None or entry.id > prev.id:
                collapsed[key] = entry
        rows: list[dict[str, object]] = []
        for entry in collapsed.values():
            req = self._requests.get(entry.request_id, {})
            # Mirror the real LEFT JOIN to album_quality_evidence: prefer
            # evidence-derived measurements over the legacy denorm columns.
            ev = self._evidence_by_id.get(entry.candidate_evidence_id) \
                if entry.candidate_evidence_id is not None else None
            ev_measurement = ev.measurement if ev is not None else None
            ev_v0 = ev.v0_metric if ev is not None else None
            spectral_grade = (
                ev_measurement.spectral_grade if ev_measurement is not None
                else None
            ) or entry.extra.get("spectral_grade")
            spectral_bitrate = (
                ev_measurement.spectral_bitrate_kbps
                if ev_measurement is not None else None
            ) or entry.extra.get("spectral_bitrate")
            v0_probe_kind = (
                (
                    V0_PROBE_LOSSLESS_SOURCE
                    if (
                        ev_v0 is not None
                        and ev_v0.subject == EVIDENCE_SUBJECT_SOURCE
                    )
                    else V0_PROBE_NATIVE_LOSSY_RESEARCH
                    if ev_v0 is not None
                    else None
                )
            ) or entry.extra.get("v0_probe_kind")
            v0_probe_avg_bitrate = (
                ev_v0.avg_bitrate_kbps if ev_v0 is not None else None
            ) or entry.extra.get("v0_probe_avg_bitrate")
            rows.append({
                "download_log_id": entry.id,
                "request_id": entry.request_id,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "mb_release_id": req.get("mb_release_id"),
                "mb_release_group_id": req.get("mb_release_group_id"),
                "soulseek_username": entry.soulseek_username,
                "validation_result": entry.validation_result,
                "spectral_grade": spectral_grade,
                "spectral_bitrate": spectral_bitrate,
                "v0_probe_kind": v0_probe_kind,
                "v0_probe_avg_bitrate": v0_probe_avg_bitrate,
                "evidence_source_codec": (
                    ev.codec if ev is not None else None
                ),
                "evidence_source_container": (
                    ev.container if ev is not None else None
                ),
                "evidence_storage_format": (
                    ev.storage_format if ev is not None else None
                ),
                "evidence_target_format": (
                    ev.target_format if ev is not None else None
                ),
                "evidence_target_is_cbr": (
                    ev.target_is_cbr if ev is not None else None
                ),
                "evidence_lineage_version": (
                    ev.lineage_version if ev is not None else None
                ),
                "evidence_min_bitrate": (
                    ev_measurement.min_bitrate_kbps
                    if ev_measurement is not None else None
                ),
                "evidence_avg_bitrate": (
                    ev_measurement.avg_bitrate_kbps
                    if ev_measurement is not None else None
                ),
                "evidence_verified_lossless": (
                    ev is not None and ev.verified_lossless_proof is not None
                ),
                "request_status": req.get("status"),
                "request_min_bitrate": req.get("min_bitrate"),
                "request_verified_lossless": req.get("verified_lossless"),
                "request_current_spectral_grade": req.get(
                    "current_spectral_grade"),
                "request_current_spectral_bitrate": req.get(
                    "current_spectral_bitrate"),
                "request_imported_path": req.get("imported_path"),
            })
        rows.sort(key=lambda r: (
            r["request_id"], -int(r["download_log_id"])))  # type: ignore[arg-type, operator]
        return rows

    def clear_wrong_match_path(self, log_id: int) -> bool:
        """Strip ``failed_path`` from a download_log row's validation_result.

        Returns True when the entry was found and carried a failed_path.
        """
        for entry in self.download_logs:
            if entry.id != log_id:
                continue
            vr = self._validation_result_dict(entry.validation_result)
            if not vr or "failed_path" not in vr:
                return False
            new_vr = {k: v for k, v in vr.items() if k != "failed_path"}
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            return True
        return False

    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int:
        """Strip ``failed_path`` from rejected rows for request/path pairs."""
        paths = {str(path) for path in failed_paths if path}
        if not paths:
            return 0
        cleared = 0
        for entry in self.download_logs:
            if entry.request_id != request_id or entry.outcome != "rejected":
                continue
            vr = self._validation_result_dict(entry.validation_result)
            if not vr or vr.get("failed_path") not in paths:
                continue
            new_vr = {k: v for k, v in vr.items() if k != "failed_path"}
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            cleared += 1
        return cleared

    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: WrongMatchTriageAudit,
    ) -> bool:
        for entry in self.download_logs:
            if entry.id != log_id:
                continue
            vr = self._validation_result_dict(entry.validation_result) or {}
            new_vr = dict(vr)
            # Mirror the real writer: msgspec encode honours omit_defaults.
            new_vr["wrong_match_triage"] = msgspec.json.decode(
                msgspec.json.encode(triage_result))
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(new_vr)
            else:
                entry.validation_result = new_vr
            return True
        return False

    @staticmethod
    def _validation_result_dict(vr: Any) -> dict[str, Any] | None:
        if isinstance(vr, dict):
            return vr
        if isinstance(vr, str):
            try:
                parsed = json.loads(vr)
            except (json.JSONDecodeError, ValueError):
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

    # --- Search log ---

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error",
                   candidates: list[CandidateScore] | None = None,
                   variant: str | None = None,
                   final_state: str | None = None,
                   browse_time_s: float = 0.0,
                   match_time_s: float = 0.0,
                   peers_browsed: int = 0,
                   peers_browsed_lazy: int = 0,
                   fanout_waves: int = 0,
                   pre_filter_skip_count: int = 0,
                   rejection_reason: str | None = None,
                   result_count_uncapped: int | None = None,
                   query_token_count: int | None = None,
                   query_distinct_token_count: int | None = None,
                   expected_track_count: int | None = None,
                   matcher_score_top1: float | None = None,
                   query_template: str | None = None) -> None:
        """Mirror PipelineDB.log_search wire boundary.

        ``candidates`` is encoded via ``msgspec.json.encode`` (same as the
        real DB writer) and stored as a JSON string so tests can decode it
        with ``msgspec.convert(json.loads(row.candidates), type=list[CandidateScore])``
        — the same path U7 will use to read the JSONB blob back.

        U11 forensics kwargs (R22-R27) mirror the production signature.
        Each defaults to ``None`` so legacy ``log_search`` calls in
        tests stay backwards-compatible.
        """
        self._next_search_log_id += 1
        candidates_json: str | None = None
        if candidates is not None:
            import msgspec
            candidates_json = msgspec.json.encode(candidates).decode()
        self.search_logs.append(SearchLogRow(
            request_id=request_id,
            query=query,
            result_count=result_count,
            elapsed_s=elapsed_s,
            outcome=outcome,
            id=self._next_search_log_id,
            candidates=candidates_json,
            variant=variant,
            final_state=final_state,
            browse_time_s=browse_time_s,
            match_time_s=match_time_s,
            peers_browsed=peers_browsed,
            peers_browsed_lazy=peers_browsed_lazy,
            fanout_waves=fanout_waves,
            pre_filter_skip_count=pre_filter_skip_count,
            rejection_reason=rejection_reason,
            result_count_uncapped=result_count_uncapped,
            query_token_count=query_token_count,
            query_distinct_token_count=query_distinct_token_count,
            expected_track_count=expected_track_count,
            matcher_score_top1=matcher_score_top1,
            query_template=query_template,
        ))

    def get_search_history(self,
                           request_id: int) -> list[dict[str, object]]:
        return [
            self._search_log_to_dict(e)
            for e in reversed(self.search_logs)
            if e.request_id == request_id
        ]

    # Production's ``get_search_plan_stats_history`` SELECTs a NARROW,
    # hand-listed column set (search_plan.py) — it deliberately excludes
    # ``candidates`` AND the U11 forensics columns (pre_filter_skip_count,
    # rejection_reason, result_count_uncapped, query_token_count,
    # query_distinct_token_count, expected_track_count, matcher_score_top1,
    # query_template) so inspection stats don't drag the wide row. The
    # fake MUST mirror that exact projection (#546 W1 parity).
    _SEARCH_PLAN_STATS_HISTORY_KEYS: tuple[str, ...] = (
        "id", "request_id", "query", "result_count", "elapsed_s", "outcome",
        "variant", "final_state", "browse_time_s", "match_time_s",
        "peers_browsed", "peers_browsed_lazy", "fanout_waves",
        "plan_id", "plan_item_id", "plan_ordinal", "plan_strategy",
        "plan_canonical_query_key", "plan_repeat_group",
        "plan_generator_id", "execution_stage", "attempt_consumed",
        "cursor_update_status", "stale_reason", "plan_cycle_snapshot",
        "created_at",
    )

    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, object]]:
        rows = self.get_search_history(request_id)
        return [
            {k: row[k] for k in self._SEARCH_PLAN_STATS_HISTORY_KEYS}
            for row in rows
        ]

    def get_search_history_page(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> "SearchLogHistoryPage":
        """Mirror of ``PipelineDB.get_search_history_page``.

        Returns at most ``limit`` rows ``id DESC``; sets
        ``next_before_id`` to the trimmed +1 row's id when a next page
        exists. Same ``id <= before_id`` resume semantics as the real DB
        so the cursor never loses a row at page boundaries.
        """
        from lib.pipeline_db import SearchLogHistoryPage as _Page
        # Walk newest-first; respect ``id <= before_id`` so the cursor
        # round-trip resumes exactly at the trimmed row.
        rows: list[dict[str, object]] = []
        for entry in reversed(self.search_logs):
            if entry.request_id != request_id:
                continue
            if before_id is not None and entry.id > before_id:
                continue
            rows.append(self._search_log_to_dict(entry))
            if len(rows) >= int(limit) + 1:
                break
        next_before_id: int | None = None
        if len(rows) > int(limit):
            extra = rows.pop()
            extra_id = extra["id"]
            assert isinstance(extra_id, int)
            next_before_id = extra_id
        return _Page(rows=rows, next_before_id=next_before_id)

    def get_saturation_summary(
        self, request_id: int, *, window_days: int = 14,
    ) -> "SaturationSummary":
        """U7 mirror of ``PipelineDB.get_saturation_summary``.

        Replicates the SQL aggregate against ``self.search_logs``:
        rows whose ``final_state`` contains ``LimitReached`` count as
        saturated; ``pre_filter_skip_count`` is summed. The window cut
        uses Python ``datetime`` arithmetic so tests can rewind
        ``SearchLogRow.created_at`` deterministically.

        ``saturation_rate`` is ``0.0`` (not NaN) when the window
        contains no rows — same explicit fallback the real DB returns.
        """
        from lib.pipeline_db import SaturationSummary as _SatSummary
        cutoff = _utcnow() - timedelta(days=int(window_days))
        total = 0
        saturated = 0
        skips = 0
        for entry in self.search_logs:
            if entry.request_id != request_id:
                continue
            if entry.created_at <= cutoff:
                continue
            total += 1
            if entry.final_state is not None and "LimitReached" in entry.final_state:
                saturated += 1
            skips += int(entry.pre_filter_skip_count or 0)
        rate = (saturated / total) if total > 0 else 0.0
        return _SatSummary(
            total_searches=total,
            saturated_searches=saturated,
            saturation_rate=rate,
            total_pre_filter_skips=skips,
            window_days=int(window_days),
        )

    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, object]]]:
        legacy = [
            self._search_log_to_dict(e)
            for e in reversed(self.search_logs)
            if e.request_id == request_id and e.plan_id is None
        ]
        head = [
            {
                "id": row.get("id"),
                "request_id": row.get("request_id"),
                "query": row.get("query"),
                "result_count": row.get("result_count"),
                "elapsed_s": row.get("elapsed_s"),
                "outcome": row.get("outcome"),
                "variant": row.get("variant"),
                "final_state": row.get("final_state"),
                "created_at": row.get("created_at"),
            }
            for row in legacy[:limit]
        ]
        return len(legacy), head

    @staticmethod
    def _search_log_to_dict(entry: SearchLogRow) -> dict[str, object]:
        # Match production JSONB read behaviour: psycopg2 deserializes
        # ``search_log.candidates`` (JSONB) into a Python list/dict on
        # ``SELECT *``. The fake stores the encoded JSON string, so decode
        # here so consumers (e.g. the U7 web route + CLI) see the same
        # parsed-list shape they get from the real DB.
        candidates: object | None
        if entry.candidates is None:
            candidates = None
        else:
            import json as _json
            candidates = _json.loads(entry.candidates)
        return {
            "id": entry.id,
            "request_id": entry.request_id,
            "query": entry.query,
            "result_count": entry.result_count,
            "elapsed_s": entry.elapsed_s,
            "outcome": entry.outcome,
            "created_at": entry.created_at,
            "candidates": candidates,
            "variant": entry.variant,
            "final_state": entry.final_state,
            "browse_time_s": entry.browse_time_s,
            "match_time_s": entry.match_time_s,
            "peers_browsed": entry.peers_browsed,
            "peers_browsed_lazy": entry.peers_browsed_lazy,
            "fanout_waves": entry.fanout_waves,
            # U1 plan-context fields. Mirror the real DB SELECT shape -- a
            # historical row writes through ``log_search`` keeps these as
            # None so legacy tests stay green.
            "plan_id": entry.plan_id,
            "plan_item_id": entry.plan_item_id,
            "plan_ordinal": entry.plan_ordinal,
            "plan_strategy": entry.plan_strategy,
            "plan_canonical_query_key": entry.plan_canonical_query_key,
            "plan_repeat_group": entry.plan_repeat_group,
            "plan_generator_id": entry.plan_generator_id,
            "execution_stage": entry.execution_stage,
            "attempt_consumed": entry.attempt_consumed,
            "cursor_update_status": entry.cursor_update_status,
            "stale_reason": entry.stale_reason,
            "plan_cycle_snapshot": entry.plan_cycle_snapshot,
            "pre_filter_skip_count": entry.pre_filter_skip_count,
            # U11 forensics columns. Same NULL semantics as production.
            "rejection_reason": entry.rejection_reason,
            "result_count_uncapped": entry.result_count_uncapped,
            "query_token_count": entry.query_token_count,
            "query_distinct_token_count": entry.query_distinct_token_count,
            "expected_track_count": entry.expected_track_count,
            "matcher_score_top1": entry.matcher_score_top1,
            "query_template": entry.query_template,
        }

    # --- slskd events cursor (issue #146 phase 1) ---

    def get_slskd_event_cursor(self) -> dict[str, Any] | None:
        cursor = self._slskd_event_cursor
        return dict(cursor) if cursor is not None else None

    def upsert_slskd_event_cursor(
        self,
        last_event_id: str,
        last_event_timestamp: str,
    ) -> None:
        self._slskd_event_cursor = {
            "last_event_id": last_event_id,
            "last_event_timestamp": last_event_timestamp,
            "updated_at": _utcnow(),
        }

    # --- slskd search-id write-ahead ledger (migration 044, issue #576) ---

    def record_search_id(
        self,
        search_id: str,
        purpose: str,
        request_id: int | None,
    ) -> None:
        row = SearchLedgerRow(
            search_id=search_id, purpose=purpose, request_id=request_id)
        self.record_search_id_calls.append(row)
        # ON CONFLICT DO NOTHING: the first insert for a given search_id
        # sticks; a re-record of the same id is a call-recording event
        # only, not a table mutation.
        self._search_ledger.setdefault(search_id, row)

    def get_unswept_search_ids(self, older_than: datetime) -> list[dict[str, Any]]:
        rows = [
            r for r in self._search_ledger.values()
            if r.deleted_at is None and r.created_at < older_than
        ]
        rows.sort(key=lambda r: r.created_at)
        return [
            {
                "search_id": r.search_id,
                "created_at": r.created_at,
                "purpose": r.purpose,
                "request_id": r.request_id,
            }
            for r in rows
        ]

    def mark_search_ids_deleted(self, search_ids: list[str]) -> None:
        now = _utcnow()
        for search_id in search_ids:
            row = self._search_ledger.get(search_id)
            if row is not None:
                row.deleted_at = now

    def prune_search_ledger(self, deleted_before: datetime) -> int:
        to_remove = [
            search_id for search_id, r in self._search_ledger.items()
            if r.deleted_at is not None and r.deleted_at < deleted_before
        ]
        for search_id in to_remove:
            del self._search_ledger[search_id]
        return len(to_remove)

    # --- slskd transfer write-ahead ownership ledger (migration 045,
    # issue #571) ---

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

    def confirm_transfer_enqueue(self, username: str, filename: str) -> int:
        candidates = [
            row for row in self._transfer_ledger.values()
            if row.username == username and row.filename == filename
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

    def get_owned_local_paths(self) -> set[str]:
        return {
            r.local_path for r in self._transfer_ledger.values()
            if r.local_path is not None
        }

    def get_owned_attempt_folders(self) -> list[dict[str, Any]]:
        """Mirrors the real INNER JOIN to ``album_requests`` -- a
        ``request_id`` with no matching seeded row (hard-deleted
        elsewhere) drops out, same as the real query."""
        seen: set[tuple[int, str]] = set()
        result: list[dict[str, Any]] = []
        for row in self._transfer_ledger.values():
            if row.attempt_fingerprint is None or row.accepted_at is None:
                continue
            key = (row.request_id, row.attempt_fingerprint)
            if key in seen:
                continue
            request = self._requests.get(row.request_id)
            if request is None:
                continue
            seen.add(key)
            result.append({
                "request_id": row.request_id,
                "attempt_fingerprint": row.attempt_fingerprint,
                "artist_name": request.get("artist_name"),
                "album_title": request.get("album_title"),
                "year": request.get("year"),
            })
        return result

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

    # --- User cooldowns ---

    def add_cooldown(self, username: str, cooldown_until: datetime,
                     reason: str | None = None) -> None:
        """Upsert a cooldown keyed by username."""
        existing = self.user_cooldowns.get(username)
        created_at = existing.created_at if existing is not None else _utcnow()
        self.user_cooldowns[username] = UserCooldownRow(
            username=username,
            cooldown_until=cooldown_until,
            reason=reason,
            created_at=created_at,
        )

    def get_cooled_down_users(self) -> list[str]:
        now = _utcnow()
        return [
            c.username for c in self.user_cooldowns.values()
            if c.cooldown_until > now
        ]

    # --- Persisted search plans (U1) ---

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        set_active: bool = True,
    ) -> int:
        if not items:
            raise ValueError(
                "create_successful_search_plan requires at least one item; "
                "use create_failed_search_plan for empty results.")
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        if self._requests[request_id].get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        # Mirror the partial unique index "one active plan per request".
        if set_active:
            for existing in self.search_plans.values():
                if (existing.request_id == request_id
                        and existing.status == PLAN_STATUS_ACTIVE):
                    raise ValueError(
                        f"request {request_id} already has an active plan; "
                        "use supersede_search_plan_with_replacement to replace it")
        # Snapshot per-item ordinals are unique by definition of the input
        # ordering; mirror the (plan, ordinal) UNIQUE constraint.
        seen_ords: set[int] = set()
        for it in items:
            if it.ordinal in seen_ords:
                raise ValueError(
                    f"duplicate plan ordinal {it.ordinal}")
            if not it.query.strip():
                raise ValueError("plan items require non-empty queries")
            seen_ords.add(it.ordinal)

        self._next_search_plan_id += 1
        plan_id = self._next_search_plan_id
        self.search_plans[plan_id] = _FakeSearchPlanRow(
            id=plan_id,
            request_id=request_id,
            generator_id=generator_id,
            status=PLAN_STATUS_ACTIVE,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        for it in items:
            self._next_search_plan_item_id += 1
            self.search_plan_items[self._next_search_plan_item_id] = (
                _FakeSearchPlanItemRow(
                    id=self._next_search_plan_item_id,
                    plan_id=plan_id,
                    ordinal=it.ordinal,
                    strategy=it.strategy,
                    query=it.query,
                    canonical_query_key=it.canonical_query_key,
                    repeat_group=it.repeat_group,
                    provenance=copy.deepcopy(it.provenance)
                        if it.provenance is not None else None,
                )
            )
        if set_active:
            row = self._requests[request_id]
            row["active_plan_id"] = plan_id
            row["next_plan_ordinal"] = 0
            row["plan_cycle_count"] = 0
            row["updated_at"] = _utcnow()
        return plan_id

    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> int:
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        if self._requests[request_id].get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        status = (
            PLAN_STATUS_FAILED_TRANSIENT if transient
            else PLAN_STATUS_FAILED_DETERMINISTIC
        )
        self._next_search_plan_id += 1
        plan_id = self._next_search_plan_id
        self.search_plans[plan_id] = _FakeSearchPlanRow(
            id=plan_id,
            request_id=request_id,
            generator_id=generator_id,
            status=status,
            failure_class=failure_class,
            error_message=error_message,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        return plan_id

    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> int:
        if not items:
            raise ValueError(
                "supersede_search_plan_with_replacement requires items.")
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        row = self._requests[request_id]
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        old_id = row.get("active_plan_id")
        now = _utcnow()
        if old_id is not None:
            old = self.search_plans.get(old_id)
            if old is not None:
                old.status = PLAN_STATUS_SUPERSEDED
                old.superseded_at = now

        # Bypass the "no active plan" guard since we just demoted the old one.
        self._next_search_plan_id += 1
        new_id = self._next_search_plan_id
        self.search_plans[new_id] = _FakeSearchPlanRow(
            id=new_id,
            request_id=request_id,
            generator_id=generator_id,
            status=PLAN_STATUS_ACTIVE,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        for it in items:
            self._next_search_plan_item_id += 1
            self.search_plan_items[self._next_search_plan_item_id] = (
                _FakeSearchPlanItemRow(
                    id=self._next_search_plan_item_id,
                    plan_id=new_id,
                    ordinal=it.ordinal,
                    strategy=it.strategy,
                    query=it.query,
                    canonical_query_key=it.canonical_query_key,
                    repeat_group=it.repeat_group,
                    provenance=copy.deepcopy(it.provenance)
                        if it.provenance is not None else None,
                )
            )
        if old_id is not None:
            old = self.search_plans.get(old_id)
            if old is not None:
                old.superseded_by_plan_id = new_id
        row["active_plan_id"] = new_id
        row["next_plan_ordinal"] = 0
        row["plan_cycle_count"] = 0
        row["updated_at"] = now
        return new_id

    def _items_for_plan(self, plan_id: int) -> list[SearchPlanItemRow]:
        rows = [
            it for it in self.search_plan_items.values()
            if it.plan_id == plan_id
        ]
        rows.sort(key=lambda r: r.ordinal)
        return [
            SearchPlanItemRow(
                id=r.id,
                plan_id=r.plan_id,
                ordinal=r.ordinal,
                strategy=r.strategy,
                query=r.query,
                canonical_query_key=r.canonical_query_key,
                repeat_group=r.repeat_group,
                provenance=(
                    SearchPlanItemProvenance(
                        values=msgspec.convert(
                            copy.deepcopy(r.provenance),
                            type=dict[str, Any],
                        )
                    )
                    if r.provenance is not None else None
                ),
            )
            for r in rows
        ]

    def _plan_to_row(self, plan: _FakeSearchPlanRow) -> SearchPlanRow:
        return SearchPlanRow(
            id=plan.id,
            request_id=plan.request_id,
            generator_id=plan.generator_id,
            status=plan.status,
            failure_class=plan.failure_class,
            metadata_snapshot=(
                msgspec.convert(
                    copy.deepcopy(plan.metadata_snapshot),
                    type=SearchPlanMetadataSnapshot,
                )
                if plan.metadata_snapshot is not None else None
            ),
            provenance=(
                SearchPlanProvenance(
                    values=msgspec.convert(
                        copy.deepcopy(plan.provenance),
                        type=dict[str, Any],
                    )
                )
                if plan.provenance is not None else None
            ),
            error_message=plan.error_message,
            superseded_at=plan.superseded_at,
            superseded_by_plan_id=plan.superseded_by_plan_id,
            created_at=plan.created_at,
        )

    def get_active_search_plan(
        self,
        request_id: int,
    ) -> ActiveSearchPlan | None:
        row = self._requests.get(request_id)
        if row is None:
            return None
        plan_id = row.get("active_plan_id")
        if plan_id is None:
            return None
        plan = self.search_plans.get(plan_id)
        if plan is None:
            return None
        return ActiveSearchPlan(
            plan=self._plan_to_row(plan),
            items=self._items_for_plan(plan_id),
            next_ordinal=int(row.get("next_plan_ordinal") or 0),
            cycle_count=int(row.get("plan_cycle_count") or 0),
        )

    def advance_search_plan_cursor(
        self,
        request_id: int,
        *,
        target_ordinal: int,
        plan_item_count: int,
    ) -> tuple[int, int, int]:
        """Mirror of ``PipelineDB.advance_search_plan_cursor``.

        Forward-only operator-driven cursor advance. Validates inputs the
        same way the real method does, raising ``ValueError`` for missing
        request, no active plan, out-of-range target, or backward intent.
        """
        if plan_item_count <= 0:
            raise ValueError(
                f"plan_item_count must be > 0 (got {plan_item_count})")
        if target_ordinal < 0 or target_ordinal >= plan_item_count:
            raise ValueError(
                f"target_ordinal {target_ordinal} out of range "
                f"[0, {plan_item_count})")
        row = self._requests.get(request_id)
        if row is None:
            raise ValueError(f"request {request_id} not found")
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        active_plan_id = row.get("active_plan_id")
        if active_plan_id is None:
            raise ValueError(
                f"request {request_id} has no active plan")
        previous_ordinal = int(row.get("next_plan_ordinal") or 0)
        if target_ordinal <= previous_ordinal:
            raise ValueError(
                f"target_ordinal {target_ordinal} must be greater than "
                f"current next_plan_ordinal {previous_ordinal} "
                "(advance is forward-only; use regenerate for backward "
                "intent)")
        row["next_plan_ordinal"] = target_ordinal
        # Cursor-mutation recorder for the U13 R20 runtime guard.
        self.advance_search_plan_cursor_calls.append(
            (request_id, previous_ordinal, int(target_ordinal)),
        )
        return (int(active_plan_id), previous_ordinal, target_ordinal)

    def list_wanted_for_plan_reconciliation(
        self,
    ) -> list[WantedReconciliationCandidate]:
        out: list[WantedReconciliationCandidate] = []
        for rid in sorted(self._requests.keys()):
            r = self._requests[rid]
            if r.get("status") != "wanted":
                continue
            plan_id = r.get("active_plan_id")
            gen_id: str | None = None
            if plan_id is not None:
                plan = self.search_plans.get(plan_id)
                if plan is not None and plan.status == PLAN_STATUS_ACTIVE:
                    gen_id = plan.generator_id
                else:
                    plan_id = None
            out.append(WantedReconciliationCandidate(
                request_id=rid,
                active_plan_id=plan_id,
                active_plan_generator_id=gen_id,
                next_plan_ordinal=int(r.get("next_plan_ordinal") or 0),
                plan_cycle_count=int(r.get("plan_cycle_count") or 0),
            ))
        return out

    def list_search_plan_classification_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, DryRunPlanClassification]:
        """Mirror of ``PipelineDB.list_search_plan_classification_for_requests``.

        Walks ``self.search_plans`` once and returns the latest failed
        deterministic / transient generator id per request. Empty input
        returns ``{}`` without scanning.
        """
        if not request_ids:
            return {}
        # Initialise so requests with no failed plan rows still surface
        # in the result with None/None generator ids.
        out: dict[int, DryRunPlanClassification] = {
            int(rid): DryRunPlanClassification(
                request_id=int(rid),
                latest_failed_deterministic_generator_id=None,
                latest_failed_transient_generator_id=None,
                latest_failed_transient_created_at=None,
            )
            for rid in request_ids
        }
        for rid in out.keys():
            det_matches = [
                p for p in self.search_plans.values()
                if p.request_id == rid
                and p.status == PLAN_STATUS_FAILED_DETERMINISTIC
            ]
            trans_matches = [
                p for p in self.search_plans.values()
                if p.request_id == rid
                and p.status == PLAN_STATUS_FAILED_TRANSIENT
            ]
            det_matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            trans_matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            out[rid] = DryRunPlanClassification(
                request_id=rid,
                latest_failed_deterministic_generator_id=(
                    det_matches[0].generator_id if det_matches else None),
                latest_failed_transient_generator_id=(
                    trans_matches[0].generator_id if trans_matches else None),
                latest_failed_transient_created_at=(
                    trans_matches[0].created_at if trans_matches else None),
            )
        return out

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Mirror of ``PipelineDB.get_wanted_searchable``.

        Returns wanted rows that are due (same backoff gate as
        ``get_wanted``) AND have an active plan whose generator id
        matches ``generator_id``. Rows without a current-generator
        active plan are filtered out.
        """
        snapshot_at = now or _utcnow()
        blacklist = tuple(term.lower() for term in title_blacklist if term)
        eligible: list[dict[str, Any]] = []
        for r in self._requests.values():
            if r.get("status") != "wanted":
                continue
            if (
                r.get("next_retry_after") is not None
                and r["next_retry_after"] > snapshot_at
            ):
                continue
            plan_id = r.get("active_plan_id")
            if plan_id is None:
                continue
            plan = self.search_plans.get(plan_id)
            if plan is None:
                continue
            if plan.status != "active":
                continue
            if plan.generator_id != generator_id:
                continue
            if any(
                entry.source == "youtube"
                and entry.outcome == "youtube_running"
                and entry.request_id == r.get("id")
                for entry in self.download_logs
            ):
                continue
            if any(
                row.get("job_type") == IMPORT_JOB_YOUTUBE
                and row.get("request_id") == r.get("id")
                and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
                for row in self._import_jobs
            ):
                continue
            title = str(r.get("album_title") or "").lower()
            if any(term in title for term in blacklist):
                continue
            eligible.append(r)
        if limit is None:
            return [copy.deepcopy(r) for r in eligible]
        page_size = int(limit)
        slots = search_cohort_slots(page_size)
        cutoff = snapshot_at - timedelta(hours=NEW_REQUEST_PRIORITY_HOURS)
        new = [
            row for row in eligible
            if self._as_utc(_as_datetime(row.get("created_at"))) > cutoff
        ]
        established = [
            row for row in eligible
            if self._as_utc(_as_datetime(row.get("created_at"))) <= cutoff
        ]
        selected = new[:slots.new] + established[:slots.established]
        selected_ids = {int(row["id"]) for row in selected}
        remaining = [
            row for row in eligible
            if int(row["id"]) not in selected_ids
        ]
        selected.extend(remaining[:max(page_size - len(selected), 0)])
        return [copy.deepcopy(r) for r in selected]

    def get_search_plan_inspection(
        self,
        request_id: int,
    ) -> SearchPlanInspection:
        active = self.get_active_search_plan(request_id)

        def _latest(status: str) -> SearchPlanRow | None:
            matches = [
                p for p in self.search_plans.values()
                if p.request_id == request_id and p.status == status
            ]
            if not matches:
                return None
            matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            return self._plan_to_row(matches[0])

        superseded = sum(
            1 for p in self.search_plans.values()
            if p.request_id == request_id
            and p.status == PLAN_STATUS_SUPERSEDED
        )
        legacy = sum(
            1 for r in self.search_logs
            if r.request_id == request_id and r.plan_id is None
        )
        return SearchPlanInspection(
            request_id=request_id,
            active=active,
            latest_failed_deterministic=_latest(
                PLAN_STATUS_FAILED_DETERMINISTIC),
            latest_failed_transient=_latest(PLAN_STATUS_FAILED_TRANSIENT),
            superseded_count=superseded,
            legacy_search_log_count=legacy,
        )

    def get_search_plan_stats(
        self,
        request_id: int,
        *,
        current_only: bool = True,
        prefetched_history: list[dict[str, Any]] | None = None,
    ):
        """Mirror of ``PipelineDB.get_search_plan_stats``.

        Re-uses the production aggregation helper so the fake stays in
        lock-step with PostgreSQL behavior — the only thing that
        differs is where the rows come from.
        """
        from lib.pipeline_db import _build_stats_bucket, SearchPlanStats
        active = self.get_active_search_plan(request_id)
        active_plan_id = active.plan.id if active is not None else None

        history = (prefetched_history if prefetched_history is not None
                   else self.get_search_history(request_id))
        plan_aware = [r for r in history if r.get("plan_id") is not None]
        legacy = [r for r in history if r.get("plan_id") is None]
        current_rows = (
            [r for r in plan_aware if r.get("plan_id") == active_plan_id]
            if active_plan_id is not None else []
        )
        if current_only:
            other_rows: list[dict[str, Any]] = []
            other_legacy: list[dict[str, Any]] = []
        else:
            other_rows = [r for r in plan_aware
                          if r.get("plan_id") != active_plan_id]
            other_legacy = legacy
        current_bucket = _build_stats_bucket(
            plan_aware_rows=current_rows, legacy_rows=[],
            include_legacy_bucket=False,
        )
        other_bucket = _build_stats_bucket(
            plan_aware_rows=other_rows, legacy_rows=other_legacy,
            include_legacy_bucket=True,
        )
        return SearchPlanStats(
            request_id=request_id,
            current=current_bucket,
            superseded_and_legacy=other_bucket,
        )

    def record_consumed_search_attempt(
        self,
        attempt: ConsumedAttemptInput,
    ) -> ConsumedAttemptResult:
        # Cursor-mutation recorder for the U13 R20 runtime guard.
        self.record_consumed_search_attempt_calls.append(attempt)
        row = self._requests.get(attempt.request_id)
        if row is None:
            raise ValueError(f"request {attempt.request_id} not found")

        active_plan_id = row.get("active_plan_id")
        next_ordinal = int(row.get("next_plan_ordinal") or 0)
        cycle_count = int(row.get("plan_cycle_count") or 0)
        plan = self.search_plans.get(attempt.plan_id)
        item = self.search_plan_items.get(attempt.plan_item_id)
        if (
            plan is None
            or plan.request_id != attempt.request_id
            or item is None
            or item.plan_id != attempt.plan_id
        ):
            raise ValueError(
                f"plan_item_id={attempt.plan_item_id} does not belong to "
                f"plan_id={attempt.plan_id} for request_id={attempt.request_id}")
        is_stale = (
            row.get("status") == "replaced"
            or active_plan_id != attempt.plan_id
            or next_ordinal != attempt.plan_ordinal
            or cycle_count != attempt.cycle_count_snapshot
        )

        # Snapshot pre-write so a partial mutation can be unwound on
        # validation failure, mirroring the real DB transaction.
        snapshot_request = copy.deepcopy(row)
        snapshot_log_count = len(self.search_logs)
        snapshot_next_id = self._next_search_log_id

        try:
            if is_stale:
                cursor_update_status = CURSOR_UPDATE_STALE
                execution_stage = SEARCH_LOG_STAGE_STALE_COMPLETION
                stale_reason: str | None = (
                    "request_replaced"
                    if row.get("status") == "replaced"
                    else "regenerated"
                )
                new_next_ordinal = next_ordinal
                new_cycle = cycle_count
            else:
                execution_stage = SEARCH_LOG_STAGE_ACCEPTED
                stale_reason = None
                count = max(int(attempt.plan_item_count), 0)
                if count == 0:
                    cursor_update_status = CURSOR_UPDATE_ADVANCED
                    new_next_ordinal = next_ordinal + 1
                    new_cycle = cycle_count
                elif attempt.plan_ordinal >= count - 1:
                    cursor_update_status = CURSOR_UPDATE_WRAPPED
                    new_next_ordinal = 0
                    new_cycle = cycle_count + 1
                else:
                    cursor_update_status = CURSOR_UPDATE_ADVANCED
                    new_next_ordinal = next_ordinal + 1
                    new_cycle = cycle_count

            self._next_search_log_id += 1
            log_id = self._next_search_log_id
            self.search_logs.append(SearchLogRow(
                request_id=attempt.request_id,
                query=attempt.query,
                result_count=attempt.result_count,
                elapsed_s=attempt.elapsed_s,
                outcome=attempt.outcome,
                id=log_id,
                candidates=attempt.candidates_json,
                variant=attempt.variant,
                final_state=attempt.final_state,
                browse_time_s=attempt.browse_time_s,
                match_time_s=attempt.match_time_s,
                peers_browsed=attempt.peers_browsed,
                peers_browsed_lazy=attempt.peers_browsed_lazy,
                fanout_waves=attempt.fanout_waves,
                plan_id=attempt.plan_id,
                plan_item_id=attempt.plan_item_id,
                plan_ordinal=attempt.plan_ordinal,
                plan_strategy=attempt.plan_strategy,
                plan_canonical_query_key=attempt.plan_canonical_query_key,
                plan_repeat_group=attempt.plan_repeat_group,
                plan_generator_id=attempt.plan_generator_id,
                execution_stage=execution_stage,
                attempt_consumed=not is_stale,
                cursor_update_status=cursor_update_status,
                stale_reason=stale_reason,
                plan_cycle_snapshot=attempt.cycle_count_snapshot,
                pre_filter_skip_count=attempt.pre_filter_skip_count,
                rejection_reason=attempt.rejection_reason,
                result_count_uncapped=attempt.result_count_uncapped,
                query_token_count=attempt.query_token_count,
                query_distinct_token_count=attempt.query_distinct_token_count,
                expected_track_count=attempt.expected_track_count,
                matcher_score_top1=attempt.matcher_score_top1,
                query_template=attempt.query_template,
            ))

            now = _utcnow()
            if not is_stale:
                row["next_plan_ordinal"] = new_next_ordinal
                row["plan_cycle_count"] = new_cycle
                row["updated_at"] = now
                if (
                    attempt.apply_scheduler_attempt
                    and not attempt.scheduler_success
                ):
                    new_count = (row.get("search_attempts") or 0) + 1
                    row["search_attempts"] = new_count
                    row["last_attempt_at"] = now
                    backoff_minutes = min(
                        BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                        BACKOFF_MAX_MINUTES,
                    )
                    row["next_retry_after"] = (
                        now + timedelta(minutes=backoff_minutes))
                elif (
                    attempt.apply_scheduler_attempt
                    and attempt.scheduler_success
                ):
                    row["last_attempt_at"] = now

                # U12: mirror the wrap-time failure_class write in
                # ``PipelineDB.record_consumed_search_attempt``. The
                # classification runs only on wrap (the cycle that
                # just completed is ``cycle_count``, pre-increment)
                # and only overwrites ``failure_class`` when the
                # classifier returns a non-None verdict — degenerate
                # cycles (zero consumed attempts) preserve the prior
                # value.
                if cursor_update_status == CURSOR_UPDATE_WRAPPED:
                    summaries = [
                        _SearchSummary(
                            outcome=str(lr.outcome),
                            rejection_reason=lr.rejection_reason,
                        )
                        for lr in self.search_logs
                        if (
                            lr.request_id == attempt.request_id
                            and lr.plan_cycle_snapshot == cycle_count
                            and bool(lr.attempt_consumed)
                        )
                    ]
                    verdict = _classify_failure_class(
                        summaries,
                        current_status=str(row.get("status") or "wanted"),
                    )
                    if verdict is not None:
                        row["failure_class"] = verdict
                        row["updated_at"] = now
            return ConsumedAttemptResult(
                search_log_id=log_id,
                cursor_update_status=cursor_update_status,
                new_next_ordinal=new_next_ordinal,
                new_cycle_count=new_cycle,
                is_stale=is_stale,
            )
        except Exception:
            # Roll back the partial mutation so test assertions can prove
            # "log-without-cursor or cursor-without-log" never happens.
            self._requests[attempt.request_id] = snapshot_request
            self.search_logs = self.search_logs[:snapshot_log_count]
            self._next_search_log_id = snapshot_next_id
            raise

    def record_non_consuming_search_attempt(
        self,
        attempt: NonConsumingAttemptInput,
    ) -> int:
        # Cursor-adjacent recorder for the U13 R20 runtime guard. This
        # method does not advance the cursor itself, but it does write
        # ``search_log`` with plan context — the detection job must not
        # call it either (the probe is its own slskd surface and never
        # touches ``search_log``).
        self.record_non_consuming_search_attempt_calls.append(attempt)
        row = self._requests.get(attempt.request_id)
        if row is None:
            raise ValueError(f"request {attempt.request_id} not found")
        cycle_snapshot = int(row.get("plan_cycle_count") or 0)
        self._next_search_log_id += 1
        log_id = self._next_search_log_id
        self.search_logs.append(SearchLogRow(
            request_id=attempt.request_id,
            query=attempt.query,
            result_count=attempt.result_count,
            elapsed_s=attempt.elapsed_s,
            outcome=attempt.outcome,
            id=log_id,
            final_state=attempt.final_state,
            plan_id=attempt.plan_id,
            plan_item_id=attempt.plan_item_id,
            plan_ordinal=attempt.plan_ordinal,
            plan_strategy=attempt.plan_strategy,
            plan_canonical_query_key=attempt.plan_canonical_query_key,
            plan_repeat_group=attempt.plan_repeat_group,
            plan_generator_id=attempt.plan_generator_id,
            execution_stage=SEARCH_LOG_STAGE_PRE_ATTEMPT,
            attempt_consumed=False,
            cursor_update_status=CURSOR_UPDATE_UNCHANGED,
            plan_cycle_snapshot=cycle_snapshot,
            pre_filter_skip_count=attempt.pre_filter_skip_count,
            rejection_reason=attempt.rejection_reason,
            result_count_uncapped=attempt.result_count_uncapped,
            query_token_count=attempt.query_token_count,
            query_distinct_token_count=attempt.query_distinct_token_count,
            expected_track_count=attempt.expected_track_count,
            matcher_score_top1=attempt.matcher_score_top1,
            query_template=attempt.query_template,
        ))
        if attempt.apply_scheduler_attempt and row.get("status") != "replaced":
            now = _utcnow()
            new_count = (row.get("search_attempts") or 0) + 1
            row["search_attempts"] = new_count
            row["last_attempt_at"] = now
            backoff_minutes = min(
                BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                BACKOFF_MAX_MINUTES,
            )
            row["next_retry_after"] = now + timedelta(
                minutes=backoff_minutes)
            row["updated_at"] = now
        return log_id

    def assert_log(self, test: Any, index: int, **expected: Any) -> None:
        """Assert fields on a download_log entry at the given index.

        Usage: db.assert_log(self, 0, outcome="success", request_id=42)
        """
        test.assertGreater(len(self.download_logs), index,
                           f"Expected at least {index + 1} download_log entries, "
                           f"got {len(self.download_logs)}")
        entry = self.download_logs[index]
        for field, value in expected.items():
            actual = getattr(entry, field, entry.extra.get(field))
            test.assertEqual(actual, value,
                             f"download_log[{index}].{field}: "
                             f"expected {value!r}, got {actual!r}")


class FakePipelineDBSource:
    """Typed stand-in for ``album_source.DatabaseSource`` / similar.

    Production calls ``ctx.pipeline_db_source._get_db()`` (and a handful of
    higher-level methods) to reach the pipeline DB. Tests historically
    constructed this with ``MagicMock`` and ``source._get_db.return_value
    = ...``; replace with this typed fake so the surface is explicit and
    the test fails loudly if production calls an unexpected method.

    Surface mirrors the production source's six public callables:
    ``_get_db``, ``get_tracks``, ``get_wanted_searchable``, ``mark_done``,
    ``reject_and_requeue``, ``close``. The fake's behavior is intentionally
    minimal — tests that exercise real DB activity should rely on the
    underlying ``FakePipelineDB`` directly (via ``source.db``).
    """

    def __init__(self, db: FakePipelineDB | None = None) -> None:
        self.db: FakePipelineDB = db if db is not None else FakePipelineDB()
        # Call records — empty unless production reached a method.
        self.get_tracks_calls: list[Any] = []
        self.mark_done_calls: list[dict[str, Any]] = []
        self.reject_and_requeue_calls: list[dict[str, Any]] = []
        self.close_calls: int = 0
        # Test-configurable returns for the wanted iterator. Default empty
        # so the worker pipeline observes "nothing to do."
        self._wanted_searchable: list[Any] = []

    def _get_db(self) -> FakePipelineDB:
        return self.db

    def get_tracks(self, album_record: Any) -> list[TrackRecord]:
        self.get_tracks_calls.append(album_record)
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return []
        rows = self.db._tracks.get(request_id, [])
        album_id = request_id * -1
        out: list[TrackRecord] = []
        for t in rows:
            out.append({
                "title": t["title"],
                "trackNumber": str(t.get("track_number") or ""),
                "mediumNumber": t["disc_number"],
                "duration": int((t.get("length_seconds") or 0) * 10_000_000),
                "id": 0,
                "albumId": album_id,
            })
        return out

    def set_wanted_searchable(self, records: list[Any]) -> None:
        """Configure what ``get_wanted_searchable`` returns."""
        self._wanted_searchable = list(records)

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
    ) -> list[Any]:
        del generator_id, title_blacklist
        if limit is None:
            return list(self._wanted_searchable)
        return list(self._wanted_searchable[:limit])

    def mark_done(
        self,
        album_record: Any,
        bv_result: Any,
        dest_path: Any = None,
        download_info: Any = None,
        import_job_id: int | None = None,
    ) -> Any:
        call = {
            "album_record": album_record,
            "bv_result": bv_result,
            "dest_path": dest_path,
            "download_info": download_info,
        }
        if import_job_id is not None:
            call["import_job_id"] = import_job_id
        self.mark_done_calls.append(call)
        if import_job_id is None or self.db.get_import_job(import_job_id) is None:
            return None
        from lib.dispatch import _do_mark_done
        from lib.quality import DownloadInfo

        request_id = getattr(album_record, "db_request_id", None)
        if not isinstance(request_id, int):
            return None
        dl_info = (
            download_info
            if isinstance(download_info, DownloadInfo)
            else DownloadInfo()
        )
        return _do_mark_done(
            cast(Any, self.db),
            request_id,
            dl_info,
            distance=getattr(bv_result, "distance", None),
            scenario=getattr(bv_result, "scenario", None),
            dest_path=dest_path,
            detail=getattr(bv_result, "detail", None),
            import_job_id=import_job_id,
        )

    def reject_and_requeue(
        self,
        album_record: Any,
        bv_result: Any,
        usernames: Any = None,
        download_info: Any = None,
        search_filetype_override: Any = None,
        cooled_down_users: set[str] | None = None,
        import_job_id: int | None = None,
    ) -> Any:
        self.reject_and_requeue_calls.append({
            "album_record": album_record,
            "bv_result": bv_result,
            "usernames": usernames,
            "download_info": download_info,
            "search_filetype_override": search_filetype_override,
            "cooled_down_users": cooled_down_users,
        })
        if import_job_id is not None and self.db.get_import_job(import_job_id) is not None:
            from lib.dispatch import _record_rejection_and_maybe_requeue
            from lib.quality import DownloadInfo
            from lib.terminal_outcomes import (
                PendingImportTerminalOutcome,
                TerminalDenylist,
            )

            request_id = getattr(album_record, "db_request_id", None)
            if not isinstance(request_id, int):
                return None
            dl_info = (
                download_info
                if isinstance(download_info, DownloadInfo)
                else DownloadInfo()
            )
            pending = _record_rejection_and_maybe_requeue(
                cast(Any, self.db),
                request_id,
                dl_info,
                detail=getattr(bv_result, "detail", None),
                error=getattr(bv_result, "error", None),
                validation_result=(
                    dl_info.validation_result or bv_result.to_json()
                ),
                requeue=True,
                search_filetype_override=search_filetype_override,
                import_job_id=import_job_id,
            )
            assert isinstance(pending, PendingImportTerminalOutcome)
            return pending.append_denylists(*(
                TerminalDenylist(
                    username,
                    "beets validation rejected",
                    apply_cooldown=True,
                )
                for username in sorted(usernames or ())
            ))
        return None

    def close(self) -> None:
        self.close_calls += 1
