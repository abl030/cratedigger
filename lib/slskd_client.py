"""Typed in-repo slskd HTTP client (issue #146).

Replaces the `slskd-api` PyPI library for the six endpoints cratedigger
actually uses, plus the events endpoint the library never wrapped. The
wire contract notes below are verified against slskd 0.24.5 on doc2:

- ``GET /api/v0/events`` returns events **newest-first** with ``data`` as
  a JSON *string* — the payload is decoded a second time into a typed
  Struct. ``X-Total-Count`` carries the retained-event total. The ``type``
  query param is accepted but does NOT filter; callers filter client-side.
- Non-2xx responses raise ``requests.HTTPError`` with the response
  attached (peer-offline detection reads ``.response.text``; the search
  submitter branches on ``.response.status_code``). The error body is
  consumed *before* raising so the blocking connection pool never leaks
  the slot — the failure mode the old response-hook patching existed for.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any
import uuid
from urllib.parse import quote

import msgspec
import requests
from requests.adapters import HTTPAdapter

from lib.config import CratediggerConfig


logger = logging.getLogger("cratedigger")

SLSKD_HTTP_POOL_ADMIN_SLACK = 4
SLSKD_HTTP_TIMEOUT_S = 120.0


# === Event wire types (msgspec Structs per code-quality § wire-boundary) ===


class SlskdRawEvent(msgspec.Struct, frozen=True):
    """One row of the events envelope. ``data`` is a nested JSON string."""

    id: str
    timestamp: str
    type: str
    data: str


class SlskdEventTransfer(msgspec.Struct, rename="camel", frozen=True):
    """The transfer DTO embedded in DownloadFileComplete events."""

    id: str
    username: str
    filename: str
    size: int


class SlskdDownloadFileCompleteEvent(msgspec.Struct, rename="camel", frozen=True):
    """Decoded DownloadFileComplete payload — ``local_filename`` is the
    authoritative post-rename absolute path, including any ``_<ticks>``
    collision suffix (slskd FileService.MoveFile)."""

    local_filename: str
    remote_filename: str
    transfer: SlskdEventTransfer


class SlskdDownloadDirectoryCompleteEvent(msgspec.Struct, rename="camel", frozen=True):
    """Decoded DownloadDirectoryComplete payload — the authoritative local
    folder slskd placed a finished directory download into."""

    local_directory_name: str
    remote_directory_name: str
    username: str


class TransferSnapshot(msgspec.Struct, rename="camel", frozen=True):
    """One slskd transfer/file entry — the typed shape ``DownloadFile.status``
    holds once a poll cycle observes a matching transfer (issue #468).

    ``state`` and ``bytes_transferred`` are the two fields the poll state
    machine actually reads back off ``DownloadFile.status``
    (``lib/download.py``, ``lib/slskd_transfers.py``). The remaining
    identity/lifecycle fields (``id``, ``username``, ``filename``,
    ``size``, the four timestamps, ``percent_complete``) round out the
    wire-boundary contract so this Struct models the whole transfer
    concept slskd reports for a matched file — not a narrower ad hoc
    subset — even though nothing downstream reads them via ``.status.*``
    yet. slskd's real Transfer DTO carries many more fields still
    (``direction``, ``averageSpeed``, ``placeInQueue``, ``exception``,
    ...) that we don't model at all — msgspec ignores unknown fields by
    default (no ``forbid_unknown_fields``).

    Every field defaults: a queued transfer has no ``bytesTransferred``
    or lifecycle timestamps yet, a bare match-lookup entry may carry only
    ``filename``/``id``, and the two synthetic constructions
    (``_restored_terminal_status``, the vanished-transfer fallback in
    ``lib/download.py``) build a ``TransferSnapshot`` directly with only
    ``state`` (+ optionally ``bytes_transferred``) set.
    """

    id: str = ""
    username: str = ""
    filename: str = ""
    state: str = ""
    size: int = 0
    bytes_transferred: int = 0
    requested_at: str | None = None
    enqueued_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    percent_complete: float = 0.0


def parse_transfer_snapshot(raw: dict[str, Any]) -> TransferSnapshot | None:
    """Convert one slskd transfer/file dict — typically the winning
    candidate ``match_transfer`` selected from a shared poll-cycle
    snapshot — into a ``TransferSnapshot``.

    Returns ``None`` (logging a warning) on a ``msgspec.ValidationError``
    instead of raising. This runs inside the 5-minute poll loop against a
    snapshot shared by every in-flight album: one malformed entry must
    degrade to "no status observed this cycle" for just that one file —
    the same signal already used for "no matching transfer found" — not
    abort the whole snapshot or poll cycle. Contrast with
    ``SlskdTransfersApi.get_download``, which decodes strictly (raises):
    that is a single-item client boundary whose one production call site
    already sits inside a per-file try/except, so raising there is
    already contained at the same granularity.
    """
    try:
        return msgspec.convert(raw, type=TransferSnapshot)
    except msgspec.ValidationError:
        logger.warning(
            "slskd transfer snapshot: skipping malformed entry (keys=%s)",
            sorted(raw.keys()) if isinstance(raw, dict) else type(raw).__name__,
            exc_info=True,
        )
        return None


DOWNLOAD_FILE_COMPLETE = "DownloadFileComplete"
DOWNLOAD_DIRECTORY_COMPLETE = "DownloadDirectoryComplete"


def decode_download_file_complete(
    event: SlskdRawEvent,
) -> SlskdDownloadFileCompleteEvent:
    """Second-stage decode of a DownloadFileComplete event's data string."""
    return msgspec.json.decode(
        event.data.encode(), type=SlskdDownloadFileCompleteEvent)


def decode_download_directory_complete(
    event: SlskdRawEvent,
) -> SlskdDownloadDirectoryCompleteEvent:
    """Second-stage decode of a DownloadDirectoryComplete event's data string."""
    return msgspec.json.decode(
        event.data.encode(), type=SlskdDownloadDirectoryCompleteEvent)


@dataclass(frozen=True)
class SlskdEventsPage:
    """One page of the events feed plus the retained-event total.

    ``total_count`` is ``None`` when slskd omitted the ``X-Total-Count``
    header — callers must then rely on empty-page / cursor / page-cap
    stops rather than silently truncating the scan.
    """

    events: list[SlskdRawEvent]
    total_count: int | None


# === Client ===


class SlskdClient:
    """Typed slskd client owning its requests session and pool.

    Method names and return shapes deliberately mirror the retired
    `slskd-api` surface (dict/list returns for the legacy endpoints) so
    call sites migrate mechanically; the events endpoint — the new wire
    boundary — returns typed Structs.
    """

    def __init__(
        self,
        host: str,
        api_key: str,
        url_base: str = "/",
        *,
        timeout: float = SLSKD_HTTP_TIMEOUT_S,
        pool_size: int = 10,
    ) -> None:
        base = host.rstrip("/")
        stripped_base = url_base.strip("/")
        if stripped_base:
            base = f"{base}/{stripped_base}"
        self.api_url = f"{base}/api/v0"
        self._timeout = timeout

        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            pool_block=True,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"accept": "*/*", "X-API-Key": api_key})
        self._session = session

        self.transfers = SlskdTransfersApi(self)
        self.users = SlskdUsersApi(self)
        self.searches = SlskdSearchesApi(self)
        self.events = SlskdEventsApi(self)
        self.application = SlskdApplicationApi(self)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> requests.Response:
        response = self._session.request(
            method,
            self.api_url + path,
            params=params,
            json=json_body,
            timeout=self._timeout,
        )
        if not response.ok:
            # Consume the body BEFORE raising: returns the pooled
            # connection (pool_block=True would otherwise starve every
            # worker) and keeps `.text` readable for callers inspecting
            # the HTTPError (peer-offline detection, 429/409 retry).
            _ = response.content
            response.raise_for_status()
        return response


class SlskdTransfersApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def enqueue(self, username: str, files: list[dict[str, Any]]) -> bool:
        self._client._request(
            "POST",
            f"/transfers/downloads/{quote(username, safe='')}",
            json_body=files,
        )
        return True

    def get_all_downloads(self, includeRemoved: bool = False) -> list[dict[str, Any]]:
        response = self._client._request(
            "GET", "/transfers/downloads/",
            params={"includeRemoved": includeRemoved})
        return response.json()

    def get_download(self, username: str, id: str) -> TransferSnapshot:
        response = self._client._request(
            "GET", f"/transfers/downloads/{quote(username, safe='')}/{id}")
        return msgspec.convert(response.json(), type=TransferSnapshot)

    def cancel_download(self, username: str, id: str, remove: bool = False) -> bool:
        self._client._request(
            "DELETE",
            f"/transfers/downloads/{quote(username, safe='')}/{id}",
            params={"remove": remove},
        )
        return True

    def remove_completed_downloads(self) -> bool:
        self._client._request("DELETE", "/transfers/downloads/all/completed")
        return True


class SlskdUsersApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def status(self, username: str) -> dict[str, Any]:
        response = self._client._request(
            "GET", f"/users/{quote(username, safe='')}/status")
        return response.json()

    def directory(self, username: str, directory: str) -> list[dict[str, Any]]:
        response = self._client._request(
            "POST",
            f"/users/{quote(username, safe='')}/directory",
            json_body={"directory": directory},
        )
        return response.json()


class SlskdSearchesApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def search_text(
        self,
        searchText: str,
        id: str | None = None,
        fileLimit: int = 10000,
        filterResponses: bool = True,
        maximumPeerQueueLength: int = 1000000,
        minimumPeerUploadSpeed: int = 0,
        minimumResponseFileCount: int = 1,
        responseLimit: int = 100,
        searchTimeout: int = 15000,
    ) -> dict[str, Any]:
        """Submit a search. slskd requires a client-generated search uuid."""
        try:
            search_id = str(uuid.UUID(id)) if id else str(uuid.uuid4())
        except ValueError:
            search_id = str(uuid.uuid4())
        response = self._client._request("POST", "/searches", json_body={
            "id": search_id,
            "fileLimit": fileLimit,
            "filterResponses": filterResponses,
            "maximumPeerQueueLength": maximumPeerQueueLength,
            "minimumPeerUploadSpeed": minimumPeerUploadSpeed,
            "minimumResponseFileCount": minimumResponseFileCount,
            "responseLimit": responseLimit,
            "searchText": searchText,
            "searchTimeout": searchTimeout,
        })
        return response.json()

    def state(self, id: str, includeResponses: bool = False) -> dict[str, Any]:
        response = self._client._request(
            "GET", f"/searches/{id}",
            params={"includeResponses": includeResponses})
        return response.json()

    def search_responses(self, id: str) -> list[dict[str, Any]]:
        response = self._client._request("GET", f"/searches/{id}/responses")
        return response.json()

    def stop(self, id: str) -> bool:
        self._client._request("PUT", f"/searches/{id}")
        return True

    def delete(self, id: str) -> bool:
        self._client._request("DELETE", f"/searches/{id}")
        return True


class SlskdEventsApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def list(self, *, limit: int = 500, offset: int = 0) -> SlskdEventsPage:
        """One page of the events feed, newest-first.

        Envelope rows are converted individually: one malformed event
        (a future slskd version emitting ``data: null``, say) is skipped
        with a warning instead of failing the whole page — a page-level
        strict decode would permanently wedge the ingest cursor behind
        the poison event.
        """
        response = self._client._request(
            "GET", "/events", params={"limit": limit, "offset": offset})
        events: list[SlskdRawEvent] = []
        for row in msgspec.json.decode(response.content, type=list):
            try:
                events.append(msgspec.convert(row, type=SlskdRawEvent))
            except msgspec.ValidationError:
                logger.warning(
                    "slskd events: skipping malformed envelope row "
                    "(id=%s type=%s)",
                    row.get("id") if isinstance(row, dict) else None,
                    row.get("type") if isinstance(row, dict) else None,
                    exc_info=True,
                )
        raw_total = response.headers.get("X-Total-Count")
        total = int(raw_total) if raw_total is not None else None
        return SlskdEventsPage(events=events, total_count=total)


class SlskdApplicationApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def version(self) -> str:
        response = self._client._request("GET", "/application/version")
        return response.json()


def derive_slskd_http_pool_size(cfg: CratediggerConfig) -> int:
    """Return the minimum requests pool size for the configured pipeline width."""
    return max(
        1,
        int(cfg.browse_global_max_workers)
        + int(cfg.search_max_inflight)
        + int(cfg.page_size)
        + SLSKD_HTTP_POOL_ADMIN_SLACK,
    )
