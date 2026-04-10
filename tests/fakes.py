"""Lightweight fakes for stateful collaborators.

FakePipelineDB records state transitions, log rows, denylist entries, and
cooldowns in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from lib.pipeline_db import BACKOFF_BASE_MINUTES, BACKOFF_MAX_MINUTES, RequestSpectralStateUpdate


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    validation_result: str | None = None
    import_result: str | None = None
    # Catch-all for less commonly asserted fields
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DenylistEntry:
    """One row in source_denylist."""
    request_id: int
    username: str
    reason: str | None = None


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
        return True


class FakeSlskdUsers:
    """Stateful fake for the slskd users API."""

    def __init__(self) -> None:
        self.directory_calls: list[tuple[str, str]] = []
        self.directory_error: Exception | None = None
        self._directories: dict[tuple[str, str], list[Any]] = {}
        self._directory_errors: dict[tuple[str, str], Exception] = {}

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

    def directory(self, username: str, directory: str) -> list[Any]:
        self.directory_calls.append((username, directory))
        if self.directory_error is not None:
            raise self.directory_error
        directory_error = self._directory_errors.get((username, directory))
        if directory_error is not None:
            raise directory_error
        return copy.deepcopy(self._directories.get((username, directory), []))


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
        self.download_logs: list[DownloadLogRow] = []
        self.denylist: list[DenylistEntry] = []
        self.cooldowns_applied: list[str] = []
        self.recorded_attempts: list[tuple[int, str]] = []
        self._cooldown_result: bool | Callable[[str], bool] = False

    # --- Seeding ---

    def seed_request(self, row: dict[str, Any]) -> None:
        """Add a request row to the fake DB. Must include 'id'."""
        rid = row["id"]
        self._requests[rid] = copy.deepcopy(row)

    def request(self, request_id: int) -> dict[str, Any]:
        """Get a request row (for test assertions). Raises KeyError if missing."""
        return self._requests[request_id]

    def set_cooldown_result(self, result: bool | Callable[[str], bool]) -> None:
        """Configure what check_and_apply_cooldown returns.

        Pass a bool for a fixed result, or a callable(username) -> bool
        for per-user conditional results.
        """
        self._cooldown_result = result

    # --- PipelineDB interface methods ---

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        return copy.deepcopy(self._requests.get(request_id))

    def update_status(self, request_id: int, status: str, **extra: Any) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        row["status"] = status
        row["active_download_state"] = None
        row["updated_at"] = _utcnow()
        for key, val in extra.items():
            row[key] = val

    def reset_to_wanted(self, request_id: int, **fields: Any) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        now = _utcnow()
        row["status"] = "wanted"
        row["search_attempts"] = 0
        row["download_attempts"] = 0
        row["validation_attempts"] = 0
        row["next_retry_after"] = None
        row["last_attempt_at"] = None
        row["active_download_state"] = None
        row["updated_at"] = now
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "min_bitrate" in fields:
            current_min_bitrate = row.get("min_bitrate")
            if current_min_bitrate is not None:
                row["prev_min_bitrate"] = current_min_bitrate
            row["min_bitrate"] = fields["min_bitrate"]

    def set_downloading(self, request_id: int, state_json: str) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "wanted":
            return False
        now = _utcnow()
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        row["last_attempt_at"] = now
        row["updated_at"] = now
        return True

    def clear_download_state(self, request_id: int) -> None:
        row = self._requests.get(request_id)
        if row:
            row["active_download_state"] = None
            row["updated_at"] = _utcnow()

    def log_download(self, request_id: int, **kwargs: Any) -> None:
        named = {
            "outcome", "soulseek_username", "filetype",
            "beets_distance", "beets_scenario", "beets_detail",
            "staged_path", "error_message", "validation_result",
            "import_result",
        }
        entry_kwargs = {k: kwargs.get(k) for k in named}
        extra = {k: v for k, v in kwargs.items() if k not in named}
        self.download_logs.append(DownloadLogRow(
            request_id=request_id, **entry_kwargs, extra=extra))

    def add_denylist(self, request_id: int, username: str,
                     reason: str | None = None) -> None:
        self.denylist.append(DenylistEntry(request_id, username, reason))

    def get_denylisted_users(self, request_id: int) -> list[dict[str, Any]]:
        return [
            {"username": e.username, "reason": e.reason, "created_at": None}
            for e in self.denylist if e.request_id == request_id
        ]

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

    def get_downloading(self) -> list[dict[str, Any]]:
        return [copy.deepcopy(r) for r in self._requests.values()
                if r.get("status") == "downloading"]

    def update_request_fields(self, request_id: int, **fields: Any) -> None:
        row = self._requests.get(request_id)
        if row:
            row.update(fields)
            row["updated_at"] = _utcnow()

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
