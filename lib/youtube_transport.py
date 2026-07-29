"""Shared Requests transport factory for YouTube Music adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import requests
    from ytmusicapi import YTMusic


def build_youtube_client() -> tuple[YTMusic, requests.Session]:
    """Build the production YTMusic client and its caller-owned session."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from ytmusicapi import YTMusic

    class _DefaultTimeoutSession(requests.Session):
        def request(self, *args: Any, **kwargs: Any):
            kwargs.setdefault("timeout", (5, 30))
            return super().request(*args, **kwargs)

    session = _DefaultTimeoutSession()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return YTMusic(requests_session=session, language="en"), session
