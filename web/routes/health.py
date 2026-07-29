"""Dependency-free response writer for the pre-channel liveness route."""

from __future__ import annotations

from typing import Protocol


class _HealthHandler(Protocol):
    close_connection: bool

    def send_response_only(self, code: int) -> None: ...

    def end_headers(self) -> None: ...


def serve_healthz(handler: _HealthHandler) -> None:
    """Write the exact bare 204 liveness response."""
    # The anonymous exception must never leave an unread request body on a
    # keep-alive backend connection: BaseHTTPRequestHandler would otherwise
    # parse those bytes as a second, channel-spoofed request.
    handler.close_connection = True
    handler.send_response_only(204)
    handler.end_headers()
