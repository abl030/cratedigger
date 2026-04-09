"""Lightweight fakes for stateful collaborators.

FakePipelineDB records state transitions, log rows, denylist entries, and
cooldowns in-memory. Use it in orchestration tests to assert domain outcomes
instead of MagicMock call shapes.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any


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
        self._cooldown_result: bool = False

    # --- Seeding ---

    def seed_request(self, row: dict[str, Any]) -> None:
        """Add a request row to the fake DB. Must include 'id'."""
        rid = row["id"]
        self._requests[rid] = copy.deepcopy(row)

    def request(self, request_id: int) -> dict[str, Any]:
        """Get a request row (for test assertions). Raises KeyError if missing."""
        return self._requests[request_id]

    def set_cooldown_result(self, result: bool) -> None:
        """Configure what check_and_apply_cooldown returns."""
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
        for key, val in extra.items():
            row[key] = val

    def reset_to_wanted(self, request_id: int, **fields: Any) -> None:
        row = self._requests.get(request_id)
        if row is None:
            return
        row["status"] = "wanted"
        row["search_attempts"] = 0
        row["download_attempts"] = 0
        row["validation_attempts"] = 0
        row["next_retry_after"] = None
        row["last_attempt_at"] = None
        row["active_download_state"] = None
        if "search_filetype_override" in fields:
            row["search_filetype_override"] = fields["search_filetype_override"]
        if "min_bitrate" in fields:
            row["prev_min_bitrate"] = row.get("min_bitrate")
            row["min_bitrate"] = fields["min_bitrate"]

    def set_downloading(self, request_id: int, state_json: str) -> bool:
        row = self._requests.get(request_id)
        if row is None or row["status"] != "wanted":
            return False
        row["status"] = "downloading"
        row["active_download_state"] = state_json
        return True

    def clear_download_state(self, request_id: int) -> None:
        row = self._requests.get(request_id)
        if row:
            row["active_download_state"] = None

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
        return self._cooldown_result

    def record_attempt(self, request_id: int, attempt_type: str) -> None:
        self.recorded_attempts.append((request_id, attempt_type))
        row = self._requests.get(request_id)
        if row:
            col = f"{attempt_type}_attempts"
            row[col] = (row.get(col) or 0) + 1

    def update_request_fields(self, request_id: int, **fields: Any) -> None:
        row = self._requests.get(request_id)
        if row:
            row.update(fields)
