"""Pure request-channel and browser-origin security decisions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal
from urllib.parse import SplitResult, urlsplit

BROWSER_CHANNEL = "browser"
CLI_CHANNEL = "cli"
CHANNEL_HEADER = "X-Cratedigger-Request-Channel"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

RequestChannel = Literal["browser", "cli"]


class RequestSecurityError(ValueError):
    """The request did not satisfy the trusted-channel boundary."""


@dataclass(frozen=True)
class _Origin:
    scheme: str
    host: str
    port: int


def is_exact_liveness_request(method: str, request_target: str) -> bool:
    """Return whether this is the sole pre-channel application exception."""
    return method in {"GET", "HEAD"} and request_target == "/healthz"


def validate_canonical_origin(value: str) -> None:
    """Reject a configured origin that is not one exact HTTP(S) origin."""
    _parse_url(value, allow_path=False)


def authorize_request(
    *,
    method: str,
    channel_values: Sequence[str],
    origin_values: Sequence[str],
    referer_values: Sequence[str],
    canonical_origin: str,
) -> RequestChannel:
    """Authorize one already-parsed application request.

    Transport authority is established outside this helper. This function
    accepts only the exact channel assertion that transport wrote, then adds
    browser same-origin provenance for every method outside the explicit safe
    set.
    """
    channel = _request_channel(channel_values)
    if channel == CLI_CHANNEL or method in SAFE_METHODS:
        return channel

    expected = _parse_url(canonical_origin, allow_path=False)
    origin = _optional_single_url(
        "Origin", origin_values, allow_path=False,
    )
    referer = _optional_single_url(
        "Referer", referer_values, allow_path=True,
    )
    if origin is None and referer is None:
        raise RequestSecurityError("browser mutation requires provenance")
    for supplied in (origin, referer):
        if supplied is not None and supplied != expected:
            raise RequestSecurityError("browser provenance origin mismatch")
    return channel


def _request_channel(values: Sequence[str]) -> RequestChannel:
    if len(values) != 1:
        raise RequestSecurityError("request channel must appear exactly once")
    value = values[0]
    if value == BROWSER_CHANNEL:
        return BROWSER_CHANNEL
    if value == CLI_CHANNEL:
        return CLI_CHANNEL
    raise RequestSecurityError("unknown request channel")


def _optional_single_url(
    name: str,
    values: Sequence[str],
    *,
    allow_path: bool,
) -> _Origin | None:
    if not values:
        return None
    if len(values) != 1:
        raise RequestSecurityError(f"{name} must appear at most once")
    return _parse_url(values[0], allow_path=allow_path)


def _parse_url(value: str, *, allow_path: bool) -> _Origin:
    if not value or value != value.strip() or value.lower() == "null":
        raise RequestSecurityError("invalid origin value")
    if (
        "\\" in value
        or "," in value
        or (not allow_path and ("?" in value or "#" in value))
        or any(ord(char) <= 0x20 or ord(char) == 0x7F for char in value)
    ):
        raise RequestSecurityError("invalid origin value")
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise RequestSecurityError("invalid origin value") from exc
    _validate_url_shape(parsed, allow_path=allow_path)
    try:
        port = parsed.port
    except ValueError as exc:
        raise RequestSecurityError("invalid origin port") from exc
    scheme = parsed.scheme.lower()
    if port is None:
        port = 80 if scheme == "http" else 443
    hostname = parsed.hostname
    if hostname is None:
        raise RequestSecurityError("origin host is required")
    return _Origin(scheme=scheme, host=hostname.lower(), port=port)


def _validate_url_shape(parsed: SplitResult, *, allow_path: bool) -> None:
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.netloc:
        raise RequestSecurityError("origin must be HTTP or HTTPS")
    if parsed.netloc.endswith(":"):
        raise RequestSecurityError("origin port is incomplete")
    if parsed.username is not None or parsed.password is not None:
        raise RequestSecurityError("origin credentials are forbidden")
    if not allow_path and (parsed.path or parsed.query or parsed.fragment):
        raise RequestSecurityError("Origin must contain only an origin")
