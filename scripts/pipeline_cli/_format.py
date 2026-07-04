"""Shared display/formatting helpers for the pipeline-cli package (#495).

Small, pure, dependency-free renderers used across several command-family
modules (query, show, quality, search-plan, triage, replace, beets-distance,
long-tail). Mirrors the ``web/routes/_pydantic.py`` convention of a leading-
underscore shared module for cross-route helpers that don't belong to any
single command family.
"""

from datetime import date, datetime, time
from decimal import Decimal


def _json_default(value):
    """Serialize common PostgreSQL values for JSON/debug output."""
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _fmt_br(kbps):
    """Format a bitrate value for display."""
    if kbps is None:
        return "-"
    return f"{kbps}kbps"


def _fmt_measurement(m, label=""):
    """Format an AudioQualityMeasurement dict for display."""
    if not m:
        return f"{label}(none)"
    parts = [_fmt_br(m.get("min_bitrate_kbps"))]
    if m.get("spectral_grade"):
        sg = m["spectral_grade"]
        if m.get("spectral_bitrate_kbps"):
            sg += f" ~{m['spectral_bitrate_kbps']}kbps"
        parts.append(f"spectral={sg}")
    if m.get("verified_lossless"):
        parts.append("verified_lossless")
    if m.get("was_converted_from"):
        parts.append(f"from {m['was_converted_from']}")
    if m.get("is_cbr"):
        parts.append("CBR")
    return f"{label}{', '.join(parts)}"


def _truncate(text: str, width: int) -> str:
    """Truncate ``text`` to ``width`` characters, marking with an ellipsis.

    Pure helper used by ``triage list``'s human-readable table renderer.
    Avoids pulling in textwrap for a 4-line helper.
    """
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _format_dt(value: object) -> str:
    """Compact display of a datetime/date/time for table cells.

    Uses the same ISO 8601 rendering ``_json_default`` emits, but strips
    sub-second precision so the table stays narrow. Returns ``"-"`` on
    ``None`` so empty cells are visually distinct from a zero-length
    string. ``object`` is wider than necessary, but the helper is used
    against ``msgspec.to_builtins`` output which is statically untyped.
    """
    if value is None:
        return "-"
    if isinstance(value, (date, datetime, time)):
        iso = value.isoformat()
        # Drop microseconds + timezone marker for table compactness.
        if "." in iso:
            iso = iso.split(".", 1)[0]
        if iso.endswith("+00:00"):
            iso = iso[:-6] + "Z"
        return iso
    return str(value)
