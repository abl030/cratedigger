"""slskd client configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from lib.config import CratediggerConfig


logger = logging.getLogger("cratedigger")

SLSKD_HTTP_POOL_ADMIN_SLACK = 4
SLSKD_HTTP_TIMEOUT_S = 120.0


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
