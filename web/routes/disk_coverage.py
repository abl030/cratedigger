"""Disk coverage API route."""

import msgspec
import re

from lib.disk_coverage_service import disk_coverage


def _server():
    """Deferred import to avoid circular deps."""
    from web import server
    return server


def _truthy(params: dict[str, list[str]], key: str) -> bool:
    return params.get(key, ["0"])[0].lower() in {"1", "true", "yes", "on"}


def get_disk_coverage(h, params: dict[str, list[str]]) -> None:
    include_rows = not (
        params.get("include_rows", ["1"])[0].lower() in {"0", "false", "no", "off"}
    )
    beets = _server()._beets_db()
    if beets is None:
        h._error("Beets DB not configured", 503)
        return
    result = disk_coverage(
        _server()._db(),
        beets,
        include_rows=include_rows,
        include_inverse=_truthy(params, "inverse"),
    )
    h._json(msgspec.to_builtins(result))


GET_ROUTES: dict[str, object] = {
    "/api/disk-coverage": get_disk_coverage,
}

GET_PATTERNS: list[tuple[re.Pattern[str], object]] = []

GET_DESCRIPTIONS: dict[str, str] = {
    "/api/disk-coverage": (
        "Exact-ID reconciliation of active pipeline rows against beets "
        "disk presence, with optional inverse beets-only rows."
    ),
}

PATTERN_DESCRIPTIONS: list[tuple[re.Pattern[str], str]] = []
