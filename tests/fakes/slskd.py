"""Stateful fakes for the slskd API client."""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from lib.slskd_client import DownloadUser

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
        self.cancel_download_calls: list[CancelDownloadCall] = []
        self.enqueue_result = True
        self.enqueue_error: Exception | None = None
        self.get_all_downloads_error: Exception | None = None
        self.cancel_download_error: Exception | None = None
        self.cancel_download_result = True

    def enqueue(self, username: str, files: list[dict[str, Any]]) -> bool:
        self.enqueue_calls.append(EnqueueCall(username, copy.deepcopy(files)))
        if self.enqueue_error is not None:
            raise self.enqueue_error
        return self.enqueue_result

    def get_all_downloads(self, includeRemoved: bool = False) -> list[DownloadUser]:
        from lib.slskd_client import DownloadUser, parse_downloads_envelope
        self.get_all_downloads_calls.append(includeRemoved)
        self._api.call_log.append("transfers.get_all_downloads")
        if self.get_all_downloads_error is not None:
            raise self.get_all_downloads_error
        # Mirror production's decode exactly (test-fidelity Rule B):
        # SlskdTransfersApi.get_all_downloads() runs the raw JSON through
        # parse_downloads_envelope() before returning — the fake must do
        # the same so tests exercise the identical typed shape (#507).
        return parse_downloads_envelope(self._api._next_download_snapshot())

    def cancel_download(self, username: str, id: str,
                        remove: bool = False) -> bool:
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
        self.status_calls: list[str] = []
        self._statuses: dict[str, str] = {}
        self._status_errors: dict[str, Exception] = {}
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

    def set_status(self, username: str, presence: str) -> None:
        """Configure the presence returned by ``status(username)``.

        ``presence`` is the slskd-api ``UserPresence`` value -- one of
        ``"Online"``, ``"Away"``, ``"Offline"``.
        """
        self._statuses[username] = presence

    def set_status_error(self, username: str, error: Exception) -> None:
        """Configure ``status(username)`` to raise ``error``."""
        self._status_errors[username] = error

    def status(self, username: str) -> dict[str, Any]:
        """Mirror slskd-api ``UsersApi.status`` shape: ``{"presence": ...,
        "isPrivileged": False}``. Default presence is ``"Online"`` so legacy
        tests that don't configure status stay green."""
        self.status_calls.append(username)
        configured_error = self._status_errors.get(username)
        if configured_error is not None:
            raise configured_error
        presence = self._statuses.get(username, "Online")
        return {"presence": presence, "isPrivileged": False}


@dataclass
class SearchTextCall:
    """One slskd ``searches.search_text`` call captured by FakeSlskdSearches."""
    search_text: str
    kwargs: dict[str, Any]


class FakeSlskdSearches:
    """Stateful fake for the slskd searches API.

    Drives orchestration tests over `search_for_album` / `_submit_plan_search`:
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

    def _maybe_apply_post_stop_flip(self, cfg: dict[str, Any]) -> None:
        """Apply the post-stop state/response flip on the first observation
        after ``stop()``. Called from both ``state()`` and
        ``search_responses()`` because slskd's real async cleanup commits
        the response store regardless of which endpoint we poll. Pre-#242
        the flip was state()-gated; that no longer matches the production
        helper which polls responses directly.
        """
        if cfg.get("_stopped") and cfg.get("post_stop_state") is not None:
            cfg["state"] = cfg["post_stop_state"]
            cfg["post_stop_state"] = None  # only flip once
            if cfg.get("post_stop_responses") is not None:
                cfg["responses"] = cfg["post_stop_responses"]
                cfg["response_count"] = len(cfg["responses"])
                cfg["post_stop_responses"] = None

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
        self._maybe_apply_post_stop_flip(cfg)
        return {
            "id": search_id,
            "state": cfg["state"],
            "isComplete": True,
            "responseCount": cfg.get("response_count", 0),
        }

    def search_responses(self, search_id: Any) -> list[dict[str, Any]]:
        self.responses_calls.append(search_id)
        cfg = self._searches.get(search_id)
        if cfg is None:
            return []
        self._maybe_apply_post_stop_flip(cfg)
        return copy.deepcopy(cfg["responses"])

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


class FakeSlskdEvents:
    """Stateful fake for the slskd events API (issue #146).

    Seed with ``set_events(events_newest_first)``; ``list`` slices the
    seeded feed exactly like slskd's offset/limit pagination and reports
    ``total_count`` from the seeded feed length (overridable via
    ``total_count_override`` for retention/pruning scenarios).
    """

    def __init__(self, api: "FakeSlskdAPI | None" = None) -> None:
        from lib.slskd_client import SlskdEventsPage, SlskdRawEvent
        self._api = api
        self._page_cls = SlskdEventsPage
        self._event_cls = SlskdRawEvent
        self._events: list[Any] = []
        self.total_count_override: int | None = None
        # Mirror slskd omitting the X-Total-Count header entirely
        # (page.total_count = None).
        self.omit_total_count = False
        self.list_calls: list[tuple[int, int]] = []
        self.list_error: Exception | None = None

    def set_events(self, events: list[Any]) -> None:
        """Seed the feed, newest-first (index 0 = most recent)."""
        self._events = list(events)

    def make_event(self, *, id: str, timestamp: str, type: str, data: str) -> Any:
        return self._event_cls(id=id, timestamp=timestamp, type=type, data=data)

    def list(self, *, limit: int = 500, offset: int = 0) -> Any:
        self.list_calls.append((limit, offset))
        if self._api is not None:
            self._api.call_log.append("events.list")
        if self.list_error is not None:
            raise self.list_error
        total: int | None
        if self.omit_total_count:
            total = None
        elif self.total_count_override is not None:
            total = self.total_count_override
        else:
            total = len(self._events)
        return self._page_cls(
            events=self._events[offset:offset + limit],
            total_count=total,
        )


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
        self.events = FakeSlskdEvents(self)
        # Cross-sub-API call ordering, for tests that pin sequencing
        # (e.g. snapshot-before-ingest in poll_active_downloads).
        self.call_log: list[str] = []
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


