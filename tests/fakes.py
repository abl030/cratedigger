"""Lightweight fakes for stateful collaborators.

FakePipelineDB records state transitions, log rows, denylist entries, and
cooldowns in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.
"""

from __future__ import annotations

import copy
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator

if TYPE_CHECKING:
    from lib.quality import CandidateScore

from lib.import_queue import (
    ImportJob,
    IMPORT_JOB_PREVIEW_DISABLED_MESSAGE,
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_PREVIEW_WOULD_IMPORT,
    import_preview_enabled_from_env,
    validate_job_type,
    validate_preview_failure_status,
    validate_payload,
    validate_status,
)
from lib.pipeline_db import (ActiveSearchPlan, BACKOFF_BASE_MINUTES,
                             BACKOFF_MAX_MINUTES, BadAudioHashInput,
                             BadAudioHashRow, ConsumedAttemptInput,
                             ConsumedAttemptResult, CURSOR_UPDATE_ADVANCED,
                             CURSOR_UPDATE_STALE, CURSOR_UPDATE_UNCHANGED,
                             CURSOR_UPDATE_WRAPPED, NonConsumingAttemptInput,
                             PLAN_STATUS_ACTIVE, PLAN_STATUS_FAILED_DETERMINISTIC,
                             PLAN_STATUS_FAILED_TRANSIENT,
                             PLAN_STATUS_SUPERSEDED,
                             RequestSpectralStateUpdate,
                             RequestV0ProbeStateUpdate, SEARCH_LOG_STAGE_ACCEPTED,
                             SEARCH_LOG_STAGE_PRE_ATTEMPT,
                             SEARCH_LOG_STAGE_STALE_COMPLETION,
                             SearchPlanInspection, SearchPlanItemInput,
                             SearchPlanItemRow, SearchPlanRow,
                             WantedReconciliationCandidate)
from lib.release_identity import ReleaseIdentity, normalize_release_id


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def _as_datetime(value: Any) -> datetime:
    """Normalise a timestamp-ish value to an aware ``datetime``.

    Most test rows now carry real datetimes via ``make_request_row``,
    but older hand-rolled fixtures still use ISO strings. Sorting with a
    mixed key would raise ``TypeError``; this helper collapses both
    shapes to a comparable datetime (aware, UTC) and uses ``_EPOCH`` as
    the sentinel for missing values so ordering stays deterministic.
    """
    if value is None:
        return _EPOCH
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return _EPOCH
        return parsed if parsed.tzinfo else parsed.replace(
            tzinfo=timezone.utc)
    return _EPOCH


@dataclass
class DownloadLogRow:
    """One row in download_log, captured by FakePipelineDB.log_download."""
    request_id: int
    outcome: str | None = None
    soulseek_username: str | None = None
    filetype: str | None = None
    beets_distance: float | None = None
    beets_scenario: str | None = None
    beets_detail: str | None = None
    staged_path: str | None = None
    error_message: str | None = None
    validation_result: Any = None
    import_result: Any = None
    # Auto-assigned monotonic id matching PostgreSQL serial behaviour.
    id: int = 0
    # Auto-populated timestamp matching download_log.created_at.
    created_at: datetime = field(default_factory=_utcnow)
    # Catch-all for less commonly asserted fields
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DenylistEntry:
    """One row in source_denylist."""
    request_id: int
    username: str
    reason: str | None = None


@dataclass
class SearchLogRow:
    """One row in search_log, captured by FakePipelineDB.log_search."""
    request_id: int
    query: str | None = None
    result_count: int | None = None
    elapsed_s: float | None = None
    outcome: str = "error"
    id: int = 0
    created_at: datetime = field(default_factory=_utcnow)
    # Forensic capture (U5 of search-escalation-and-forensics).
    # ``candidates`` is the JSONB blob persisted by ``log_search`` — the
    # in-memory representation is the JSON string the production code
    # would have written via ``msgspec.json.encode``. NULL on error rows.
    candidates: str | None = None
    variant: str | None = None
    final_state: str | None = None
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0
    # U1 persisted-search-plans plan-context fields. All nullable; rows
    # written via ``log_search`` keep them None to mirror historical /
    # legacy production rows.
    plan_id: int | None = None
    plan_item_id: int | None = None
    plan_ordinal: int | None = None
    plan_strategy: str | None = None
    plan_canonical_query_key: str | None = None
    plan_repeat_group: str | None = None
    plan_generator_id: str | None = None
    execution_stage: str | None = None
    attempt_consumed: bool | None = None
    cursor_update_status: str | None = None
    stale_reason: str | None = None
    plan_cycle_snapshot: int | None = None


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


@dataclass
class UserCooldownRow:
    """One row in user_cooldowns, captured by FakePipelineDB.add_cooldown."""
    username: str
    cooldown_until: datetime
    reason: str | None = None
    created_at: datetime = field(default_factory=_utcnow)


@dataclass
class EnqueueCall:
    """One slskd enqueue call captured by FakeSlskdAPI."""
    username: str
    files: list[dict[str, Any]]


@dataclass
class CancelDownloadCall:
    """One slskd cancel_download call captured by FakeSlskdAPI."""
    username: str
    id: str


class FakeSlskdTransfers:
    """Stateful fake for the slskd transfers API."""

    def __init__(self, api: "FakeSlskdAPI") -> None:
        self._api = api
        self.enqueue_calls: list[EnqueueCall] = []
        self.get_all_downloads_calls: list[bool] = []
        self.get_download_calls: list[tuple[str, str]] = []
        self.get_downloads_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.cancel_download_calls: list[CancelDownloadCall] = []
        self.enqueue_result = True
        self.enqueue_error: Exception | None = None
        self.get_all_downloads_error: Exception | None = None
        self.get_download_error: Exception | None = None
        self.cancel_download_error: Exception | None = None
        self.cancel_download_result = True

    def enqueue(self, username: str, files: list[dict[str, Any]]) -> bool:
        self.enqueue_calls.append(EnqueueCall(username, copy.deepcopy(files)))
        if self.enqueue_error is not None:
            raise self.enqueue_error
        return self.enqueue_result

    def get_all_downloads(self, includeRemoved: bool = False) -> list[dict[str, Any]]:
        self.get_all_downloads_calls.append(includeRemoved)
        if self.get_all_downloads_error is not None:
            raise self.get_all_downloads_error
        return self._api._next_download_snapshot()

    def get_downloads(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.get_downloads_calls.append((args, copy.deepcopy(kwargs)))
        return self.get_all_downloads(
            includeRemoved=bool(kwargs.get("includeRemoved", False)))

    def get_download(self, username: str, id: str) -> dict[str, Any]:
        self.get_download_calls.append((username, id))
        if self.get_download_error is not None:
            raise self.get_download_error
        transfer = self._api._find_transfer(username, id)
        if transfer is None:
            raise KeyError(f"No transfer {id!r} for {username!r}")
        return transfer

    def cancel_download(self, username: str, id: str) -> bool:
        self.cancel_download_calls.append(CancelDownloadCall(username, id))
        if self.cancel_download_error is not None:
            raise self.cancel_download_error
        if not self.cancel_download_result:
            return False
        self._api.remove_transfer(username=username, id=id)
        return True


class FakeSlskdUsers:
    """Stateful fake for the slskd users API."""

    def __init__(self) -> None:
        self.directory_calls: list[tuple[str, str]] = []
        self.directory_error: Exception | None = None
        self._directories: dict[tuple[str, str], list[Any]] = {}
        self._directory_errors: dict[tuple[str, str], Exception] = {}
        self._directory_delays: dict[tuple[str, str], float] = {}
        # Optional concurrency probe — set to a callable taking a +/-1 delta to
        # observe in-flight count (used by fan-out concurrency-cap tests).
        self.in_flight_probe: Callable[[int], None] | None = None

    def set_directory(
        self,
        username: str,
        directory: str,
        result: list[Any],
    ) -> None:
        self._directories[(username, directory)] = copy.deepcopy(result)

    def set_directory_error(
        self,
        username: str,
        directory: str,
        error: Exception,
    ) -> None:
        self._directory_errors[(username, directory)] = error

    def set_directory_delay(
        self,
        username: str,
        directory: str,
        seconds: float,
    ) -> None:
        """Sleep `seconds` inside `directory(...)` before returning the registered
        result. Lets tests exercise fan-out wave deadlines and concurrency caps
        without mocking time. Default is 0.0 (no sleep)."""
        self._directory_delays[(username, directory)] = seconds

    def directory(self, username: str, directory: str) -> list[Any]:
        self.directory_calls.append((username, directory))
        if self.in_flight_probe is not None:
            self.in_flight_probe(1)
        try:
            delay = self._directory_delays.get((username, directory), 0.0)
            if delay > 0:
                time.sleep(delay)
            if self.directory_error is not None:
                raise self.directory_error
            directory_error = self._directory_errors.get((username, directory))
            if directory_error is not None:
                raise directory_error
            return copy.deepcopy(self._directories.get((username, directory), []))
        finally:
            if self.in_flight_probe is not None:
                self.in_flight_probe(-1)


@dataclass
class SearchTextCall:
    """One slskd ``searches.search_text`` call captured by FakeSlskdSearches."""
    search_text: str
    kwargs: dict[str, Any]


class FakeSlskdSearches:
    """Stateful fake for the slskd searches API.

    Drives orchestration tests over `search_for_album` / `_submit_search`:
    pre-seed canned ``state``, ``responses`` for known search ids; record
    every ``search_text`` kwargs (especially ``responseLimit``) for later
    assertion.

    Usage:
        searches = FakeSlskdSearches()
        searches.add_search(search_id=1, state="Completed", responses=[...])
        searches.search_text_id_sequence = [1]   # next call returns id=1
        # ... drive code under test ...
        assert searches.search_text_calls[0].kwargs["responseLimit"] == 1000
    """

    def __init__(self) -> None:
        self.search_text_calls: list[SearchTextCall] = []
        self.state_calls: list[tuple[Any, bool]] = []
        self.responses_calls: list[Any] = []
        self.delete_calls: list[Any] = []
        self.stop_calls: list[Any] = []
        self.search_text_error: Exception | None = None
        # Per-search override: id -> Exception. Raised from stop() / state()
        # for that search id. Used to drive the "stop() raises" / "state()
        # raises" branches without poisoning every search.
        self._stop_errors: dict[Any, Exception] = {}
        self._stop_returns: dict[Any, bool] = {}
        # Each call returns the next id from this list; falls back to a
        # monotonically incrementing counter once the list is exhausted.
        self.search_text_id_sequence: list[Any] = []
        self._next_auto_id = 1
        # search_id -> {
        #   "state": str,
        #   "responses": list[dict],
        #   "response_count": int,
        #   "post_stop_state": str | None,
        #   "post_stop_responses": list[dict] | None,
        # }
        self._searches: dict[Any, dict[str, Any]] = {}

    def add_search(
        self,
        *,
        search_id: Any,
        state: str = "Completed",
        responses: list[dict[str, Any]] | None = None,
        response_count: int | None = None,
        post_stop_state: str | None = None,
        post_stop_responses: list[dict[str, Any]] | None = None,
    ) -> None:
        """Pre-register a canned response set for a search id.

        The watchdog reads ``state_resp["responseCount"]`` to track
        no-progress. When ``response_count`` is omitted it defaults to
        ``len(responses)``. Tests that need to simulate a stuck search with
        non-zero starting responses but no further progress can pass
        ``response_count`` explicitly.

        ``post_stop_state`` / ``post_stop_responses`` simulate slskd's async
        cleanup after ``stop()``: the next ``state()`` call after ``stop()``
        flips the state and ``search_responses()`` flips the responses.
        Leave both unset to model "slskd hung at cleanup" — state stays
        InProgress until the watchdog gives up.
        """
        self._searches[search_id] = {
            "state": state,
            "responses": copy.deepcopy(responses or []),
            "response_count": response_count if response_count is not None
                              else len(responses or []),
            "post_stop_state": post_stop_state,
            "post_stop_responses": (
                copy.deepcopy(post_stop_responses)
                if post_stop_responses is not None else None
            ),
            "_stopped": False,
        }

    def set_response_count(self, search_id: Any, count: int) -> None:
        """Mutate the responseCount of an already-seeded search.

        Tests model "responses arrive over time" by stepping the count
        between calls to advance the watchdog clock.
        """
        if search_id in self._searches:
            self._searches[search_id]["response_count"] = count

    def set_state(self, search_id: Any, state: str) -> None:
        """Mutate the state of an already-seeded search."""
        if search_id in self._searches:
            self._searches[search_id]["state"] = state

    def set_stop_error(self, search_id: Any, err: Exception) -> None:
        self._stop_errors[search_id] = err

    def set_stop_return(self, search_id: Any, value: bool) -> None:
        self._stop_returns[search_id] = value

    def search_text(self, **kwargs: Any) -> dict[str, Any]:
        text = kwargs.pop("searchText", "")
        self.search_text_calls.append(
            SearchTextCall(search_text=text, kwargs=copy.deepcopy(kwargs)))
        if self.search_text_error is not None:
            raise self.search_text_error
        if self.search_text_id_sequence:
            search_id = self.search_text_id_sequence.pop(0)
        else:
            search_id = self._next_auto_id
            self._next_auto_id += 1
        # Default state for an unconfigured id keeps the fake usable
        # without explicit ``add_search`` for one-shot tests.
        self._searches.setdefault(search_id, {
            "state": "Completed",
            "responses": [],
        })
        return {"id": search_id}

    def state(self, search_id: Any, _include_responses: bool = False) -> dict[str, Any]:
        self.state_calls.append((search_id, _include_responses))
        cfg = self._searches.get(search_id)
        if cfg is None:
            return {
                "id": search_id,
                "state": "Completed",
                "isComplete": True,
                "responseCount": 0,
            }
        # If stop() ran and a post-stop state was configured, flip on
        # the FIRST state() poll after stop(). This models slskd's async
        # cleanup landing within one poll tick.
        if cfg.get("_stopped") and cfg.get("post_stop_state") is not None:
            cfg["state"] = cfg["post_stop_state"]
            cfg["post_stop_state"] = None  # only flip once
            if cfg.get("post_stop_responses") is not None:
                cfg["responses"] = cfg["post_stop_responses"]
                cfg["response_count"] = len(cfg["responses"])
                cfg["post_stop_responses"] = None
        return {
            "id": search_id,
            "state": cfg["state"],
            "isComplete": True,
            "responseCount": cfg.get("response_count", 0),
        }

    def search_responses(self, search_id: Any) -> list[dict[str, Any]]:
        self.responses_calls.append(search_id)
        cfg = self._searches.get(search_id)
        return copy.deepcopy(cfg["responses"]) if cfg else []

    def stop(self, search_id: Any) -> bool:
        self.stop_calls.append(search_id)
        # Mark _stopped FIRST. This models slskd's real behaviour: even
        # when the wrapper call raises (e.g., transport error mid-PUT),
        # the server may have already received the cancel and started its
        # async cleanup. Tests that simulate stop() errors still need the
        # post_stop_state flip to fire so the post-cancel wait can exit
        # without spinning the deadline budget down to zero.
        cfg = self._searches.get(search_id)
        if cfg is not None:
            cfg["_stopped"] = True
        if search_id in self._stop_errors:
            raise self._stop_errors[search_id]
        return self._stop_returns.get(search_id, True)

    def delete(self, search_id: Any) -> None:
        self.delete_calls.append(search_id)


class FakeSlskdAPI:
    """In-memory fake for slskd API clients used by download tests."""

    def __init__(
        self,
        *,
        downloads: list[dict[str, Any]] | None = None,
        download_snapshots: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        self.transfers = FakeSlskdTransfers(self)
        self.users = FakeSlskdUsers()
        self.searches = FakeSlskdSearches()
        self._downloads = copy.deepcopy(downloads or [])
        self._download_snapshots = [
            copy.deepcopy(snapshot) for snapshot in (download_snapshots or [])
        ]

    def set_downloads(self, downloads: list[dict[str, Any]]) -> None:
        self._downloads = copy.deepcopy(downloads)
        self._download_snapshots = []

    def queue_download_snapshots(self, *snapshots: list[dict[str, Any]]) -> None:
        self._download_snapshots.extend(copy.deepcopy(list(snapshots)))

    def add_transfer(
        self,
        *,
        username: str,
        directory: str,
        filename: str,
        id: str,
        state: str | None = None,
        size: int | None = None,
        bytesTransferred: int | None = None,
        **extra: Any,
    ) -> None:
        group = self._find_or_create_group(username)
        directory_row = self._find_or_create_directory(group, directory)
        transfer: dict[str, Any] = {"filename": filename, "id": id}
        if state is not None:
            transfer["state"] = state
        if size is not None:
            transfer["size"] = size
        if bytesTransferred is not None:
            transfer["bytesTransferred"] = bytesTransferred
        transfer.update(extra)
        directory_row.setdefault("files", []).append(transfer)

    def _next_download_snapshot(self) -> list[dict[str, Any]]:
        if self._download_snapshots:
            self._downloads = self._download_snapshots.pop(0)
        return copy.deepcopy(self._downloads)

    def _find_transfer(self, username: str, transfer_id: str) -> dict[str, Any] | None:
        for group in self._downloads:
            if group.get("username") not in (None, "", username):
                continue
            for directory in group.get("directories", []):
                for transfer in directory.get("files", []):
                    if transfer.get("id") == transfer_id:
                        return copy.deepcopy(transfer)
        return None

    def remove_transfer(self, *, username: str, id: str) -> None:
        for group in self._downloads:
            if group.get("username") not in (None, "", username):
                continue
            for directory in group.get("directories", []):
                directory["files"] = [
                    transfer for transfer in directory.get("files", [])
                    if transfer.get("id") != id
                ]

    def _find_or_create_group(self, username: str) -> dict[str, Any]:
        for group in self._downloads:
            if group.get("username") == username:
                return group
        group = {"username": username, "directories": []}
        self._downloads.append(group)
        return group

    @staticmethod
    def _find_or_create_directory(
        group: dict[str, Any],
        directory: str,
    ) -> dict[str, Any]:
        for row in group.setdefault("directories", []):
            if row.get("directory") == directory:
                return row
        row = {"directory": directory, "files": []}
        group["directories"].append(row)
        return row


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
        self.peer_dir_observations: dict[str, dict[str, Any]] = {}
        self.user_cooldowns: dict[str, UserCooldownRow] = {}
        self.denylist: list[DenylistEntry] = []
        self.bad_audio_hashes: list[BadAudioHashRow] = []
        self._next_bad_audio_hash_id = 0
        self.cooldowns_applied: list[str] = []
        self.recorded_attempts: list[tuple[int, str]] = []
        self.status_history: list[tuple[int, str]] = []
        self.update_download_state_calls: list[tuple[int, str]] = []
        self.update_download_state_current_path_calls: list[tuple[int, str | None]] = []
        self.mark_import_subprocess_started_calls: list[tuple[int, str]] = []
        self.clear_download_state_calls: list[int] = []
        self.advisory_lock_calls: list[tuple[int, int]] = []
        self.closed = False
        self._next_request_id = 0
        self._next_download_log_id = 0
        self._next_import_job_id = 0
        self._next_search_log_id = 0
        self._cooldown_result: bool | Callable[[str], bool] = False
        self._advisory_lock_result: (
            bool | Callable[[int, int], bool]) = True
        # U1 persisted-search-plans state.
        self.search_plans: dict[int, _FakeSearchPlanRow] = {}
        self.search_plan_items: dict[int, _FakeSearchPlanItemRow] = {}
        self._next_search_plan_id = 0
        self._next_search_plan_item_id = 0

    # --- Seeding ---

    def seed_request(self, row: dict[str, Any]) -> None:
        """Add a request row to the fake DB. Must include 'id'."""
        rid = row["id"]
        self._requests[rid] = copy.deepcopy(row)
        if rid > self._next_request_id:
            self._next_request_id = rid

    def request(self, request_id: int) -> dict[str, Any]:
        """Get a request row (for test assertions). Raises KeyError if missing."""
        return self._requests[request_id]

    def set_cooldown_result(self, result: bool | Callable[[str], bool]) -> None:
        """Configure what check_and_apply_cooldown returns.

        Pass a bool for a fixed result, or a callable(username) -> bool
        for per-user conditional results.
        """
        self._cooldown_result = result

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
        preview_enabled: bool | None = None,
    ) -> ImportJob:
        validate_job_type(job_type)
        payload = validate_payload(job_type, payload or {})
        preview_enabled = (
            import_preview_enabled_from_env()
            if preview_enabled is None
            else preview_enabled
        )
        if dedupe_key is not None:
            existing = self.get_import_job_by_dedupe_key(dedupe_key)
            if existing is not None:
                return ImportJob.from_row(existing.to_dict(), deduped=True)

        self._next_import_job_id += 1
        now = _utcnow()
        preview_completed_at = None if preview_enabled else now
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
            "preview_status": (
                IMPORT_JOB_PREVIEW_WAITING
                if preview_enabled
                else IMPORT_JOB_PREVIEW_WOULD_IMPORT
            ),
            "preview_result": None,
            "preview_message": (
                None if preview_enabled else IMPORT_JOB_PREVIEW_DISABLED_MESSAGE
            ),
            "preview_error": None,
            "preview_attempts": 0,
            "preview_worker_id": None,
            "preview_started_at": None,
            "preview_heartbeat_at": None,
            "preview_completed_at": preview_completed_at,
            "importable_at": None if preview_enabled else now,
        }
        self._import_jobs.append(row)
        return ImportJob.from_row(copy.deepcopy(row))

    def get_import_job(self, job_id: int) -> ImportJob | None:
        for row in self._import_jobs:
            if row["id"] == job_id:
                return ImportJob.from_row(copy.deepcopy(row))
        return None

    def get_import_job_by_dedupe_key(
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
                or row.get("status") in ("queued", "running")
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
            if row.get("status") in ("queued", "running")
            and (request_id is None or row.get("request_id") == request_id)
        ]
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
            if row.get("status") in ("queued", "running")
        ]

        def sort_key(row: dict[str, Any]) -> tuple[int, datetime, datetime, int]:
            status = row.get("status")
            preview_status = row.get("preview_status")
            if status == "queued" and preview_status == "would_import":
                bucket = 0
            elif status == "running":
                bucket = 1
            elif status == "queued" and preview_status == "running":
                bucket = 2
            elif status == "queued" and preview_status == "waiting":
                bucket = 3
            else:
                bucket = 4
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
            and row.get("preview_status") == "would_import"
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

    def heartbeat_import_job(self, job_id: int) -> bool:
        for row in self._import_jobs:
            if row["id"] == job_id and row.get("status") == "running":
                now = _utcnow()
                row["heartbeat_at"] = now
                row["updated_at"] = now
                return True
        return False

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

    def list_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = _utcnow() - older_than
        rows = []
        for row in self._import_jobs:
            if row.get("status") != "running":
                continue
            last = _as_datetime(
                row.get("heartbeat_at")
                or row.get("started_at")
                or row.get("updated_at")
            )
            if last < cutoff:
                rows.append(row)
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def fail_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        stale = self.list_stale_running_import_jobs(
            older_than=older_than,
            limit=limit,
        )
        failed = []
        for job in stale:
            updated = self.mark_import_job_failed(
                job.id,
                error=message,
                message=message,
            )
            if updated is not None:
                failed.append(updated)
        return failed

    def requeue_running_import_jobs(
        self,
        *,
        message: str,
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
            row["status"] = "queued"
            row["message"] = message
            row["error"] = None
            row["worker_id"] = None
            row["started_at"] = None
            row["heartbeat_at"] = None
            row["updated_at"] = now
            updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
        return updated_jobs

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
                row["preview_status"] = "would_import"
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

    def list_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = _utcnow() - older_than
        rows = []
        for row in self._import_jobs:
            if row.get("status") != "queued" or row.get("preview_status") != "running":
                continue
            last = _as_datetime(
                row.get("preview_heartbeat_at")
                or row.get("preview_started_at")
                or row.get("updated_at")
            )
            if last < cutoff:
                rows.append(row)
        rows.sort(key=lambda row: (_as_datetime(row.get("updated_at")), row["id"]))
        return [ImportJob.from_row(copy.deepcopy(row)) for row in rows[:limit]]

    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        stale = self.list_stale_import_preview_jobs(
            older_than=older_than,
            limit=limit,
        )
        updated_jobs = []
        for job in stale:
            for row in self._import_jobs:
                if row["id"] != job.id:
                    continue
                now = _utcnow()
                row["preview_status"] = "waiting"
                row["preview_message"] = message
                row["preview_error"] = None
                row["preview_worker_id"] = None
                row["preview_started_at"] = None
                row["preview_heartbeat_at"] = None
                row["updated_at"] = now
                updated_jobs.append(ImportJob.from_row(copy.deepcopy(row)))
                break
        return updated_jobs

    # --- PipelineDB interface methods ---

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        return copy.deepcopy(self._requests.get(request_id))

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

    def update_status(self, request_id: int, status: str, **extra: Any) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        row["status"] = status
        row["active_download_state"] = None
        row["updated_at"] = _utcnow()
        for key, val in extra.items():
            row[key] = val
        self.status_history.append((request_id, status))

    def update_imported_path_by_release_id(
        self,
        *,
        mb_albumid: str,
        discogs_albumid: str,
        new_path: str,
    ) -> int:
        """Stand-in for ``PipelineDB.update_imported_path_by_release_id``.

        Mirrors the prod cross-layout matching (Codex R2 P2 fix):

        - ``mb_albumid`` matches ONLY the pipeline's ``mb_release_id``
          column (MB UUIDs and legacy numerics both live there).
        - ``discogs_albumid`` matches EITHER the pipeline's
          ``discogs_release_id`` OR ``mb_release_id`` column, because
          the pipeline DB stores Discogs numerics in either column
          depending on when/how the request was created (CLAUDE.md §
          "Discogs-sourced albums": numeric IDs stored in
          ``mb_release_id`` for pipeline compat).

        Returns the number of rows updated. No-op when both inputs
        are empty.
        """
        if not mb_albumid and not discogs_albumid:
            return 0
        updated = 0
        for row in self._requests.values():
            mb_hit = bool(
                mb_albumid and row.get("mb_release_id") == mb_albumid)
            discogs_hit = bool(
                discogs_albumid
                and (row.get("discogs_release_id") == discogs_albumid
                     or row.get("mb_release_id") == discogs_albumid))
            if mb_hit or discogs_hit:
                row["imported_path"] = new_path
                row["updated_at"] = _utcnow()
                updated += 1
        return updated

    def reset_to_wanted(
        self,
        request_id: int,
        *,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        now = _utcnow()
        row["status"] = "wanted"
        if clear_retry_counters:
            row["search_attempts"] = 0
            row["download_attempts"] = 0
            row["validation_attempts"] = 0
            row["next_retry_after"] = None
            row["last_attempt_at"] = None
        row["active_download_state"] = None
        row["manual_reason"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if current_min_bitrate is not None:
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        self.status_history.append((request_id, "wanted"))

    def reset_downloading_to_wanted(
        self,
        request_id: int,
        **fields: Any,
    ) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "downloading":
            return False
        now = _utcnow()
        row["status"] = "wanted"
        row["active_download_state"] = None
        row["manual_reason"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if current_min_bitrate is not None:
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]
        self.status_history.append((request_id, "wanted"))
        return True

    def set_manual(
        self,
        request_id: int,
        *,
        manual_reason: str | None = None,
    ) -> None:
        """Mirror ``PipelineDB.set_manual``: flip to ``status='manual'``,
        write ``manual_reason`` only when non-None (no NULL clobber).
        """
        row = self._requests.get(request_id)
        if row is None:
            return
        row["status"] = "manual"
        row["active_download_state"] = None
        row["updated_at"] = _utcnow()
        if manual_reason is not None:
            row["manual_reason"] = manual_reason
        self.status_history.append((request_id, "manual"))

    def reset_search_attempts(self, request_id: int) -> None:
        """Mirror ``PipelineDB.reset_search_attempts``: clear search counter,
        leave status/backoff/other counters alone.
        """
        row = self._requests.get(request_id)
        if row is None:
            return
        row["search_attempts"] = 0
        row["updated_at"] = _utcnow()

    def set_downloading(self, request_id: int, state_json: str) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "wanted":
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

    def clear_download_state(self, request_id: int) -> None:
        row = self._requests.get(request_id)
        if row:
            row["active_download_state"] = None
            row["updated_at"] = _utcnow()
        self.clear_download_state_calls.append(request_id)

    def update_download_state(self, request_id: int, state_json: str) -> None:
        row = self._requests.get(request_id)
        self.update_download_state_calls.append((request_id, state_json))
        if row:
            try:
                row["active_download_state"] = json.loads(state_json)
            except json.JSONDecodeError:
                row["active_download_state"] = state_json
            row["updated_at"] = _utcnow()

    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
    ) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "downloading":
            return False
        self.update_download_state(request_id, state_json)
        return True

    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> None:
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

    def mark_import_subprocess_started(
        self,
        request_id: int,
        timestamp: str,
    ) -> None:
        """Stamp ``import_subprocess_started_at`` on the active download
        state. No-op when the row has no ``active_download_state``
        (force/manual paths). See ``docs/advisory-locks.md``.
        """
        self.mark_import_subprocess_started_calls.append(
            (request_id, timestamp),
        )
        row = self._requests.get(request_id)
        if not row or row.get("active_download_state") is None:
            return
        state = row.get("active_download_state")
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except json.JSONDecodeError:
                return
        if not isinstance(state, dict):
            return
        state["import_subprocess_started_at"] = timestamp
        row["active_download_state"] = state
        row["updated_at"] = _utcnow()

    def log_download(self, request_id: int,
                     soulseek_username: str | None = None,
                     filetype: str | None = None,
                     download_path: str | None = None,
                     beets_distance: float | None = None,
                     beets_scenario: str | None = None,
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
                     slskd_bitrate: int | None = None,
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
                     **extra: Any) -> int:
        """Record a download_log row.

        Every parameter name matches ``PipelineDB.log_download`` exactly
        — the contract test in ``test_fakes.py`` enforces this. Only
        the 11 "first-class" fields land on ``DownloadLogRow``; the
        remaining named fields plus any test-only ``**extra`` merge into
        ``.extra`` so ``assert_log`` can still introspect them.
        """
        self._next_download_log_id += 1
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
            "slskd_bitrate": slskd_bitrate,
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
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
            beets_detail=beets_detail,
            staged_path=staged_path,
            error_message=error_message,
            validation_result=validation_result,
            import_result=import_result,
            id=self._next_download_log_id,
            extra=auxiliary,
        ))
        return self._next_download_log_id

    def abandon_auto_import_request(
        self,
        *,
        request_id: int,
        current_path: str,
        soulseek_username: str | None,
        filetype: str | None,
        beets_scenario: str,
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
        row["manual_reason"] = None
        row["updated_at"] = now
        self.status_history.append((request_id, "wanted"))
        self.record_attempt(request_id, "download")
        return self.log_download(
            request_id=request_id,
            soulseek_username=soulseek_username,
            filetype=filetype,
            beets_scenario=beets_scenario,
            beets_detail=beets_detail,
            outcome=outcome,
            staged_path=staged_path,
            error_message=error_message,
            validation_result=validation_result,
        )

    def add_denylist(self, request_id: int, username: str,
                     reason: str | None = None) -> None:
        self.denylist.append(DenylistEntry(request_id, username, reason))

    def get_denylisted_users(self, request_id: int) -> list[dict[str, Any]]:
        return [
            {"username": e.username, "reason": e.reason, "created_at": None}
            for e in self.denylist if e.request_id == request_id
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
        for row in self.bad_audio_hashes:
            if row.hash_value == hash_value and row.audio_format == audio_format:
                return row
        return None

    def has_any_bad_audio_hashes(self) -> bool:
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
    ) -> dict[str, Any] | None:
        """Most recent queued/running import job for this request, or None."""
        rows = [
            row for row in self._import_jobs
            if row.get("request_id") == request_id
            and row.get("status") in ("queued", "running")
        ]
        if not rows:
            return None
        rows.sort(key=lambda row: row["id"], reverse=True)
        return copy.deepcopy(rows[0])

    def check_and_apply_cooldown(self, username: str,
                                  config: Any = None) -> bool:  # noqa: ARG002
        self.cooldowns_applied.append(username)
        if callable(self._cooldown_result):
            return self._cooldown_result(username)
        return self._cooldown_result

    def record_attempt(self, request_id: int, attempt_type: str) -> None:
        self.recorded_attempts.append((request_id, attempt_type))
        row = self._requests.get(request_id)
        if row:
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

    def update_spectral_state(self, request_id: int,
                              update: RequestSpectralStateUpdate) -> None:
        row = self._requests.get(request_id)
        if row:
            fields = update.as_update_fields()
            row.update(fields)
            row["updated_at"] = _utcnow()

    def update_v0_probe_state(self, request_id: int,
                              update: RequestV0ProbeStateUpdate) -> None:
        row = self._requests.get(request_id)
        if row:
            fields = update.as_update_fields()
            row.update(fields)
            row["updated_at"] = _utcnow()

    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        row["verified_lossless"] = False
        row["current_spectral_grade"] = None
        row["current_spectral_bitrate"] = None
        row["current_lossless_source_v0_probe_min_bitrate"] = None
        row["current_lossless_source_v0_probe_avg_bitrate"] = None
        row["current_lossless_source_v0_probe_median_bitrate"] = None
        row["imported_path"] = None
        row["updated_at"] = _utcnow()

    def get_downloading(self) -> list[dict[str, Any]]:
        return [copy.deepcopy(r) for r in self._requests.values()
                if r.get("status") == "downloading"]

    def update_request_fields(self, request_id: int, **fields: Any) -> None:
        row = self._requests.get(request_id)
        if row:
            row.update(fields)
            row["updated_at"] = _utcnow()

    # --- Session lifecycle ---

    def close(self) -> None:
        """Record that the fake connection was closed. No-op otherwise."""
        self.closed = True

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
                    status: str = "wanted") -> int:
        """Insert an album_requests row.

        Seeds the full ``album_requests`` column set (matching
        ``make_request_row`` in ``tests/helpers.py``) so fake-backed
        tests that then read DB-defaulted fields like ``beets_distance``
        or ``*_attempts`` see the same NULL/0 defaults production
        callers get from PostgreSQL. Codex R7.
        """
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
            "country": country,
            "format": format,
            "source": source,
            "source_path": source_path,
            "reasoning": reasoning,
            "status": status,
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
            "lidarr_album_id": None,
            "lidarr_artist_id": None,
            "last_download_spectral_bitrate": None,
            "last_download_spectral_grade": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "current_lossless_source_v0_probe_min_bitrate": None,
            "current_lossless_source_v0_probe_avg_bitrate": None,
            "current_lossless_source_v0_probe_median_bitrate": None,
            "active_download_state": None,
            "manual_reason": None,
            # U1 persisted-search-plans cursor fields.
            "active_plan_id": None,
            "next_plan_ordinal": 0,
            "plan_cycle_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        return rid

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
        """Return wanted requests past their retry gate, fresh/manual rows first.

        Mirrors the real ORDER BY (no search/download/validation attempts ahead
        of the rest) but breaks ties in insertion order rather than with
        ``RANDOM()`` so tests are deterministic. Callers that care about
        specific rows within a priority bucket should assert on set membership
        rather than list order — the real DB randomises ties every cycle.
        """
        now = _utcnow()
        eligible = [
            r for r in self._requests.values()
            if r.get("status") == "wanted"
            and (r.get("next_retry_after") is None
                 or r["next_retry_after"] <= now)
        ]
        eligible.sort(
            key=lambda r: (
                0 if (
                    (r.get("search_attempts") or 0) == 0
                    and (r.get("download_attempts") or 0) == 0
                    and (r.get("validation_attempts") or 0) == 0
                ) else 1
            ))
        if limit is not None:
            eligible = eligible[:int(limit)]
        return [copy.deepcopy(r) for r in eligible]

    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None,
                ) -> list[dict[str, object]]:
        imported = {"success", "force_import"}
        rejected = {"rejected", "failed", "timeout"}
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
                "source": req.get("source"),
            })
            rows.append(joined)
            if len(rows) >= limit:
                break
        return rows

    def get_by_status(self, status: str) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(r) for r in sorted(
                (r for r in self._requests.values()
                 if r.get("status") == status),
                key=lambda r: _as_datetime(r.get("created_at")))
        ]

    def get_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return requests that have at least one download_log row."""
        with_history = {row.request_id for row in self.download_logs}
        rows = [
            r for r in self._requests.values() if r["id"] in with_history]
        # ``_as_datetime`` normalises ISO strings (from ``make_request_row``)
        # and datetimes (from ``add_request``) to one representation so
        # Python's stable sort does not raise ``TypeError`` on mixed inputs,
        # and uses a fixed epoch sentinel for missing ``updated_at`` so
        # ordering stays deterministic.
        rows.sort(
            key=lambda r: _as_datetime(r.get("updated_at")), reverse=True)
        return [copy.deepcopy(r) for r in rows[:limit]]

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
        self._tracks[request_id] = [
            {
                "disc_number": t.get("disc_number", 1),
                "track_number": t["track_number"],
                "title": t["title"],
                "length_seconds": t.get("length_seconds"),
            }
            for t in tracks
        ]

    def get_tracks(self, request_id: int) -> list[dict[str, Any]]:
        rows = list(self._tracks.get(request_id, []))
        rows.sort(key=lambda t: (t["disc_number"], t["track_number"]))
        return [copy.deepcopy(t) for t in rows]

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
    ) -> int:
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
        }
        self.cycle_metrics.append(row)
        return int(row["id"])

    def record_peer_dir_observations(
        self,
        observations: Iterable[tuple[str, str]],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        from lib.pipeline_db import _peer_dir_hashes

        observed = observed_at or _utcnow()
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        unique = {
            (str(username), str(file_dir))
            for username, file_dir in observations
            if username and file_dir
        }
        new_count = 0
        for username, file_dir in sorted(unique):
            combo_hash, username_hash, dir_hash = _peer_dir_hashes(
                username,
                file_dir,
            )
            row = self.peer_dir_observations.get(combo_hash)
            if row is None:
                self.peer_dir_observations[combo_hash] = {
                    "combo_hash": combo_hash,
                    "username_hash": username_hash,
                    "dir_hash": dir_hash,
                    "first_seen_at": observed,
                    "last_seen_at": observed,
                    "seen_count": 1,
                }
                new_count += 1
            else:
                row["last_seen_at"] = max(row["last_seen_at"], observed)
                row["seen_count"] = int(row.get("seen_count") or 0) + 1
        return new_count

    def get_peer_dir_daily_metrics(self, days: int = 14) -> dict[str, Any]:
        clamped_days = max(1, min(int(days), 90))
        rows = list(self.peer_dir_observations.values())
        return {
            "days": [
                {
                    "date": (_utcnow() - timedelta(days=idx)).date().isoformat(),
                    "new_combos": 0,
                    "new_peers": 0,
                    "new_dirs": 0,
                }
                for idx in range(clamped_days)
            ],
            "totals": {
                "known_combos": len(rows),
                "known_peers": len({row["username_hash"] for row in rows}),
                "known_dirs": len({row["dir_hash"] for row in rows}),
                "new_24h": len(rows),
                "cold_seen_24h": len(rows),
                "days_with_new": 1 if rows else 0,
                "tracked_since": (
                    min(row["first_seen_at"] for row in rows).isoformat()
                    if rows
                    else None
                ),
            },
        }

    def get_pipeline_dashboard_metrics(
        self,
        *,
        plan_generator_id: str | None = None,
    ) -> dict[str, Any]:
        if plan_generator_id is None:
            from lib.search import SEARCH_PLAN_GENERATOR_ID
            plan_generator_id = SEARCH_PLAN_GENERATOR_ID
        peer_dirs = self.get_peer_dir_daily_metrics()
        peer_dirs["heavy_queries"] = []
        peer_dirs["heavy_query_hours"] = 24
        return {
            "generated_at": _utcnow().isoformat(),
            "searches": {"windows": []},
            "cycles": {
                "windows": [],
                "recent": list(reversed(self.cycle_metrics[-12:])),
                "outliers": sorted(
                    self.cycle_metrics,
                    key=lambda row: row["cycle_total_s"],
                    reverse=True,
                )[:8],
            },
            "coverage": {},
            "peer_dirs": peer_dirs,
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
            "created_at": entry.created_at,
        }
        row.update(entry.extra)
        return row

    # --- Wrong-match review queue ---

    def get_wrong_matches(self) -> list[dict[str, object]]:
        """Rejected downloads whose ``validation_result.failed_path`` is set.

        Mirrors the real ``DISTINCT ON (request_id, failed_path)`` —
        collapse to newest per ``(request_id, failed_path)``, then sort
        newest-first within each request.
        """
        skip_scenarios = {"audio_corrupt", "spectral_reject"}
        collapsed: dict[tuple[int, str], DownloadLogRow] = {}
        for entry in self.download_logs:
            if entry.outcome != "rejected":
                continue
            vr = self._validation_result_dict(entry.validation_result)
            failed_path = vr.get("failed_path") if vr else None
            if not failed_path:
                continue
            if vr and vr.get("scenario") in skip_scenarios:
                continue
            key = (entry.request_id, str(failed_path))
            prev = collapsed.get(key)
            if prev is None or entry.id > prev.id:
                collapsed[key] = entry
        rows: list[dict[str, object]] = []
        for entry in collapsed.values():
            req = self._requests.get(entry.request_id, {})
            rows.append({
                "download_log_id": entry.id,
                "request_id": entry.request_id,
                "artist_name": req.get("artist_name"),
                "album_title": req.get("album_title"),
                "mb_release_id": req.get("mb_release_id"),
                "soulseek_username": entry.soulseek_username,
                "validation_result": entry.validation_result,
                "spectral_grade": entry.extra.get("spectral_grade"),
                "spectral_bitrate": entry.extra.get("spectral_bitrate"),
                "v0_probe_kind": entry.extra.get("v0_probe_kind"),
                "v0_probe_avg_bitrate": entry.extra.get("v0_probe_avg_bitrate"),
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

    def update_download_log_measurement(
        self,
        download_log_id: int,
        *,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        v0_probe_kind: str | None = None,
        v0_probe_avg_bitrate: int | None = None,
    ) -> bool:
        """Persist measurement evidence onto a fake download_log row.

        Mirrors ``PipelineDB.update_download_log_measurement``: partial /
        non-destructive write where ``None`` inputs leave existing values
        untouched. Stores values on ``entry.extra`` so the fake's
        ``get_wrong_matches`` (which already reads those keys) sees them.
        """
        updates: dict[str, object] = {}
        if spectral_grade is not None:
            updates["spectral_grade"] = spectral_grade
        if spectral_bitrate is not None:
            updates["spectral_bitrate"] = spectral_bitrate
        if v0_probe_kind is not None:
            updates["v0_probe_kind"] = v0_probe_kind
        if v0_probe_avg_bitrate is not None:
            updates["v0_probe_avg_bitrate"] = v0_probe_avg_bitrate
        if not updates:
            return False
        for entry in self.download_logs:
            if entry.id != download_log_id:
                continue
            entry.extra.update(updates)
            return True
        return False

    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: dict[str, object],
    ) -> bool:
        """Persist preview-driven triage audit details on a fake log row."""
        for entry in self.download_logs:
            if entry.id != log_id:
                continue
            vr = self._validation_result_dict(entry.validation_result) or {}
            vr["wrong_match_triage"] = triage_result
            if isinstance(entry.validation_result, str):
                entry.validation_result = json.dumps(vr)
            else:
                entry.validation_result = vr
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
                   fanout_waves: int = 0) -> None:
        """Mirror PipelineDB.log_search wire boundary.

        ``candidates`` is encoded via ``msgspec.json.encode`` (same as the
        real DB writer) and stored as a JSON string so tests can decode it
        with ``msgspec.convert(json.loads(row.candidates), type=list[CandidateScore])``
        — the same path U7 will use to read the JSONB blob back.
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
        ))

    def get_search_history(self,
                           request_id: int) -> list[dict[str, object]]:
        return [
            self._search_log_to_dict(e)
            for e in reversed(self.search_logs)
            if e.request_id == request_id
        ]

    def get_search_history_batch(
        self, request_ids: list[int],
    ) -> dict[int, list[dict[str, object]]]:
        wanted = set(request_ids)
        result: dict[int, list[dict[str, object]]] = {}
        for entry in reversed(self.search_logs):
            if entry.request_id not in wanted:
                continue
            result.setdefault(entry.request_id, []).append(
                self._search_log_to_dict(entry))
        return result

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
        }

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

    def get_user_cooldowns(self) -> list[dict[str, Any]]:
        rows = sorted(
            self.user_cooldowns.values(),
            key=lambda c: c.cooldown_until,
            reverse=True,
        )
        return [
            {
                "username": c.username,
                "cooldown_until": c.cooldown_until,
                "reason": c.reason,
                "created_at": c.created_at,
            }
            for c in rows
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
                provenance=copy.deepcopy(r.provenance),
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
            metadata_snapshot=copy.deepcopy(plan.metadata_snapshot),
            provenance=copy.deepcopy(plan.provenance),
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

    def is_request_plan_current(
        self,
        request_id: int,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Mirror of ``PipelineDB.is_request_plan_current`` (U5)."""
        row = self._requests.get(request_id)
        if row is None:
            return False
        if row.get("active_plan_id") != plan_id:
            return False
        if int(row.get("next_plan_ordinal") or 0) != plan_ordinal:
            return False
        if int(row.get("plan_cycle_count") or 0) != cycle_count_snapshot:
            return False
        return True

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
                if plan is not None:
                    gen_id = plan.generator_id
            out.append(WantedReconciliationCandidate(
                request_id=rid,
                active_plan_id=plan_id,
                active_plan_generator_id=gen_id,
                next_plan_ordinal=int(r.get("next_plan_ordinal") or 0),
                plan_cycle_count=int(r.get("plan_cycle_count") or 0),
            ))
        return out

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Mirror of ``PipelineDB.get_wanted_searchable``.

        Returns wanted rows that are due (same backoff gate as
        ``get_wanted``) AND have an active plan whose generator id
        matches ``generator_id``. Rows without a current-generator
        active plan are filtered out.
        """
        now = _utcnow()
        eligible: list[dict[str, Any]] = []
        for r in self._requests.values():
            if r.get("status") != "wanted":
                continue
            if r.get("next_retry_after") is not None and r["next_retry_after"] > now:
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
            eligible.append(r)
        eligible.sort(
            key=lambda r: (
                0 if (
                    (r.get("search_attempts") or 0) == 0
                    and (r.get("download_attempts") or 0) == 0
                    and (r.get("validation_attempts") or 0) == 0
                ) else 1
            ))
        if limit is not None:
            eligible = eligible[:int(limit)]
        return [copy.deepcopy(r) for r in eligible]

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
        row = self._requests.get(attempt.request_id)
        if row is None:
            raise ValueError(f"request {attempt.request_id} not found")

        active_plan_id = row.get("active_plan_id")
        next_ordinal = int(row.get("next_plan_ordinal") or 0)
        cycle_count = int(row.get("plan_cycle_count") or 0)
        is_stale = (
            active_plan_id != attempt.plan_id
            or next_ordinal != attempt.plan_ordinal
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
                stale_reason: str | None = "regenerated"
                new_next_ordinal = next_ordinal
                new_cycle = cycle_count
            else:
                # Validate plan_item_id belongs to the executing plan.
                item = self.search_plan_items.get(attempt.plan_item_id)
                if item is None or item.plan_id != attempt.plan_id:
                    raise ValueError(
                        f"plan_item_id={attempt.plan_item_id} does not "
                        f"belong to plan_id={attempt.plan_id}")
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
                attempt_consumed=True,
                cursor_update_status=cursor_update_status,
                stale_reason=stale_reason,
                plan_cycle_snapshot=cycle_count,
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
        ))
        if attempt.apply_scheduler_attempt:
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
