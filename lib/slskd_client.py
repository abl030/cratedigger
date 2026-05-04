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
