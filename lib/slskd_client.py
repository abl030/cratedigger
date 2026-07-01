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
    """One page of the events feed plus the retained-event total."""

    events: list[SlskdRawEvent]
    total_count: int


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

    def get_download(self, username: str, id: str) -> dict[str, Any]:
        response = self._client._request(
            "GET", f"/transfers/downloads/{quote(username, safe='')}/{id}")
        return response.json()

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
        """One page of the events feed, newest-first."""
        response = self._client._request(
            "GET", "/events", params={"limit": limit, "offset": offset})
        events = msgspec.json.decode(
            response.content, type=list[SlskdRawEvent])
        total = int(response.headers.get("X-Total-Count", "0"))
        return SlskdEventsPage(events=events, total_count=total)


class SlskdApplicationApi:
    def __init__(self, client: SlskdClient) -> None:
        self._client = client

    def version(self) -> str:
        response = self._client._request("GET", "/application/version")
        return response.json()


@dataclass(frozen=True)
class SlskdHttpPoolConfigResult:
    pool_size: int
    sessions_configured: int
    configured: bool


def derive_slskd_http_pool_size(cfg: CratediggerConfig) -> int:
    """Return the minimum requests pool size for the configured pipeline width."""
    return max(
        1,
        int(cfg.browse_global_max_workers)
        + int(cfg.search_max_inflight)
        + int(cfg.page_size)
        + SLSKD_HTTP_POOL_ADMIN_SLACK,
    )


def _iter_sessions(slskd_client: Any) -> list[Any]:
    """Discover unique requests.Session-like objects exposed by slskd-api."""
    sessions: list[Any] = []
    seen: set[int] = set()
    for obj in [slskd_client, *vars(slskd_client).values()] if hasattr(slskd_client, "__dict__") else [slskd_client]:
        session = getattr(obj, "session", None)
        if session is None or not hasattr(session, "adapters"):
            continue
        ident = id(session)
        if ident in seen:
            continue
        seen.add(ident)
        sessions.append(session)
    return sessions


def _adapter_for_pool(
    existing: Any,
    pool_size: int,
) -> HTTPAdapter:
    """Build a replacement adapter while preserving slskd-api timeout behavior."""
    adapter_cls: Any = existing.__class__ if existing is not None else HTTPAdapter
    kwargs: dict[str, Any] = {
        "pool_connections": pool_size,
        "pool_maxsize": pool_size,
        "pool_block": True,
    }
    timeout = getattr(existing, "timeout", None)
    if timeout is not None:
        try:
            return adapter_cls(timeout=timeout, **kwargs)
        except TypeError:
            logger.debug(
                "slskd HTTP adapter %s does not accept timeout; falling back",
                adapter_cls,
            )
    try:
        return adapter_cls(**kwargs)
    except TypeError:
        return HTTPAdapter(**kwargs)


def _pool_safe_response_hook(hook: Any) -> Any:
    if getattr(hook, "_cratedigger_pool_safe", False):
        return hook

    def wrapped(response: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return hook(response, *args, **kwargs)
        except Exception:
            close = getattr(response, "close", None)
            if callable(close):
                close()
            raise

    wrapped._cratedigger_pool_safe = True  # type: ignore[attr-defined]
    return wrapped


def _make_response_hooks_pool_safe(session: Any) -> None:
    """Close HTTP error responses so pool_block=True cannot leak slots.

    slskd-api installs a response hook that calls ``raise_for_status()``. In
    requests, response hooks run before the body is consumed; if the hook raises
    on a 500 response, urllib3 never gets the connection back. With
    ``pool_block=True`` that eventually parks every worker in ``_get_conn``.
    """
    hooks = getattr(session, "hooks", None)
    if not isinstance(hooks, dict):
        return
    response_hooks = hooks.get("response")
    if response_hooks is None:
        return
    if callable(response_hooks):
        hooks["response"] = _pool_safe_response_hook(response_hooks)
        return
    if isinstance(response_hooks, list):
        hooks["response"] = [
            _pool_safe_response_hook(hook) if callable(hook) else hook
            for hook in response_hooks
        ]


def configure_slskd_http_pool(
    slskd_client: Any,
    cfg: CratediggerConfig,
) -> SlskdHttpPoolConfigResult:
    """Configure requests connection pools on a slskd-api client.

    The installed `slskd_api.SlskdClient` exposes one shared requests session
    through each API object. This helper configures every discovered unique
    session so the 32-way browse path does not hit requests' default pool of
    10 and churn localhost connections.
    """
    pool_size = derive_slskd_http_pool_size(cfg)
    sessions = _iter_sessions(slskd_client)
    if not sessions:
        logger.warning(
            "Could not configure slskd HTTP pool: no requests session found "
            "on %s",
            type(slskd_client).__name__,
        )
        return SlskdHttpPoolConfigResult(
            pool_size=pool_size,
            sessions_configured=0,
            configured=False,
        )

    for session in sessions:
        _make_response_hooks_pool_safe(session)
        for prefix in ("http://", "https://"):
            existing = session.adapters.get(prefix)
            session.mount(prefix, _adapter_for_pool(existing, pool_size))

    logger.info(
        "Configured slskd HTTP pool: pool_size=%s sessions=%s pool_block=true",
        pool_size,
        len(sessions),
    )
    return SlskdHttpPoolConfigResult(
        pool_size=pool_size,
        sessions_configured=len(sessions),
        configured=True,
    )
