"""Pure classification functions for recents tab display.

Given a download_log row (as a LogEntry dataclass), computes a
ClassifiedEntry with badge, verdict, and summary.

No I/O, no database — fully unit-testable.
"""

import json
from dataclasses import dataclass, fields
from typing import Any, Optional

from lib.quality import ImportResult

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@dataclass
class LogEntry:
    """A download_log row, optionally joined with album_requests fields.

    Constructed from psycopg2 RealDictRow via from_row(). All bitrate
    fields are kbps unless noted otherwise.
    """
    # download_log identity
    id: int = 0
    request_id: int = 0
    outcome: str = ""
    created_at: Optional[str] = None  # ISO string after serialization

    # match result
    beets_scenario: Optional[str] = None
    beets_distance: Optional[float] = None
    beets_detail: Optional[str] = None
    soulseek_username: Optional[str] = None
    error_message: Optional[str] = None
    import_result: Optional[Any] = None
    validation_result: Optional[Any] = None

    # download quality
    filetype: Optional[str] = None
    bitrate: Optional[int] = None              # bps — the ONLY field in bps
    was_converted: bool = False
    original_filetype: Optional[str] = None
    actual_filetype: Optional[str] = None
    actual_min_bitrate: Optional[int] = None   # kbps
    slskd_filetype: Optional[str] = None
    slskd_bitrate: Optional[int] = None        # bps
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None     # kbps
    existing_min_bitrate: Optional[int] = None  # kbps
    existing_spectral_bitrate: Optional[int] = None  # kbps

    # album_requests columns (from JOIN — empty for history-only queries)
    album_title: str = ""
    artist_name: str = ""
    mb_release_id: Optional[str] = None
    request_status: Optional[str] = None
    request_min_bitrate: Optional[int] = None  # kbps
    search_filetype_override: Optional[str] = None
    source: Optional[str] = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "LogEntry":
        """Construct from a psycopg2 RealDictRow or plain dict.

        Handles datetime serialization and missing fields gracefully.
        """
        known = {f.name for f in fields(cls)}
        kwargs: dict[str, Any] = {}
        for key, value in row.items():
            if key not in known:
                continue
            # Serialize datetime objects to ISO strings
            if hasattr(value, "isoformat"):
                value = str(value.isoformat())
            kwargs[key] = value
        return cls(**kwargs)

    def to_json_dict(self) -> dict[str, Any]:
        """Convert to a plain dict suitable for JSON serialization."""
        result: dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if hasattr(value, "isoformat"):
                value = str(value.isoformat())
            result[f.name] = value
        return result


@dataclass
class ClassifiedEntry:
    """Classification result for a LogEntry — badge, verdict, and summary."""
    badge: str
    badge_class: str
    border_color: str
    verdict: str
    summary: str
    downloaded_label: str = ""  # e.g. "MP3 320", "FLAC (converted to MP3 V0)"


# ---------------------------------------------------------------------------
# Quality label
# ---------------------------------------------------------------------------

def quality_label(fmt: str, min_bitrate_kbps: int) -> str:
    """Human-readable quality label from format + bitrate in kbps.

    Examples: "MP3 V0", "MP3 320", "FLAC", "MP3 197k"
    """
    if not fmt:
        return "?"
    fmt = fmt.strip().split(",")[0].strip().upper()
    if fmt in ("FLAC", "ALAC"):
        return fmt
    if not min_bitrate_kbps or min_bitrate_kbps <= 0:
        return fmt
    if min_bitrate_kbps >= 295:
        return f"{fmt} 320"
    if min_bitrate_kbps >= 220:
        return f"{fmt} V0"
    if min_bitrate_kbps >= 170:
        return f"{fmt} V2"
    return f"{fmt} {min_bitrate_kbps}k"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_log_entry(entry: LogEntry) -> ClassifiedEntry:
    """Classify a download_log entry for display.

    Returns a ClassifiedEntry with badge, verdict, summary, and downloaded_label.
    """
    badge, badge_class, border_color, verdict = _classify(entry)
    summary = _build_summary(entry, badge, verdict)
    downloaded_label = _build_downloaded_label(entry)
    return ClassifiedEntry(
        badge=badge, badge_class=badge_class,
        border_color=border_color, verdict=verdict,
        summary=summary, downloaded_label=downloaded_label,
    )


def _classify(entry: LogEntry) -> tuple[str, str, str, str]:
    """Core classification. Returns (badge, badge_class, border_color, verdict)."""

    # --- Rejected ---
    if entry.outcome == "rejected":
        verdict = _rejection_verdict(entry)
        return ("Rejected", "badge-rejected", "#a33", verdict)

    # --- Failed / Timeout ---
    if entry.outcome in ("failed", "timeout"):
        if entry.beets_scenario == "timeout":
            verdict = "Import timed out"
        elif entry.error_message:
            verdict = f"Import error: {entry.error_message}"
        else:
            verdict = _quality_verdict_from_import_result(entry) or "Import error"
        return ("Failed", "badge-failed", "#a33", verdict)

    # --- Force import ---
    if entry.outcome == "force_import":
        return ("Force imported", "badge-force", "#46a",
                "Force imported after manual review")

    # --- Success ---
    if entry.outcome == "success":
        # Transcode scenarios
        if entry.beets_scenario in ("transcode_upgrade", "transcode_first"):
            return _classify_transcode(entry)

        is_verified_lossless = (
            entry.was_converted
            and entry.original_filetype is not None
            and entry.original_filetype.lower() == "flac"
            and entry.spectral_grade == "genuine"
        )

        # Upgrade vs new import — use existing_min_bitrate from the
        # download_log entry (what was on disk at the time of THIS download)
        had_existing = (entry.existing_min_bitrate is not None
                        and entry.existing_min_bitrate > 0)

        if had_existing:
            if entry.search_filetype_override:
                return _classify_search_filetype_override(entry, is_verified_lossless)
            verdict = _upgrade_verdict(
                entry.existing_min_bitrate,
                entry.actual_min_bitrate or entry.request_min_bitrate,
                entry.was_converted, entry.original_filetype,
                is_verified_lossless,
                actual_filetype=entry.actual_filetype)
            return ("Upgraded", "badge-upgraded", "#3a6", verdict)

        # New import
        verdict = _new_import_verdict(entry, is_verified_lossless)
        return ("Imported", "badge-new", "#1a4a2a", verdict)

    # --- Unknown outcome ---
    label = str(entry.outcome).capitalize() if entry.outcome else "Unknown"
    return (label, "badge-rejected", "#444", str(entry.outcome or "Unknown outcome"))


def _parse_import_result(entry: LogEntry) -> ImportResult | None:
    """Parse the import_result JSONB from a LogEntry, or None."""
    raw = entry.import_result
    if raw is None:
        return None
    try:
        if isinstance(raw, dict):
            return ImportResult.from_dict(raw)
        elif isinstance(raw, str):
            return ImportResult.from_json(raw)
        return None
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


def _real_bitrate_kbps(entry: LogEntry) -> int | None:
    """Best available actual file bitrate in kbps, excluding spectral.

    spectral_bitrate is a cliff estimate ("what was the original source?"),
    not the file's actual bitrate. It must never appear in non-spectral
    verdicts. This matches the chain in _build_downloaded_label.
    """
    return (entry.actual_min_bitrate
            or (entry.bitrate // 1000 if entry.bitrate else None))


def _comparison_verdict(
    new_kbps: int | None,
    old_kbps: int | None,
    prefix: str = "",
) -> str:
    """Build a '… is not better than existing …' verdict string."""
    new_s = f"{new_kbps}kbps" if new_kbps is not None else "unknown"
    old_s = f"{old_kbps}kbps" if old_kbps is not None else "unknown"
    if prefix:
        return f"{prefix} {new_s} — not better than existing {old_s}"
    return f"{new_s} is not better than existing {old_s}"


def _quality_verdict_from_import_result(entry: LogEntry) -> str | None:
    """Derive a quality comparison verdict from ImportResult JSONB.

    Used by both rejected and failed outcomes — single source of truth
    for "X is not better than Y" messages.
    """
    ir = _parse_import_result(entry)
    if ir is None:
        return None

    new_m = ir.new_measurement
    existing_m = ir.existing_measurement
    new_kbps = new_m.min_bitrate_kbps if new_m is not None else None
    old_kbps = None
    if existing_m is not None:
        old_kbps = (existing_m.min_bitrate_kbps
                    if existing_m.min_bitrate_kbps is not None
                    else existing_m.spectral_bitrate_kbps)

    if ir.decision == "downgrade":
        return _comparison_verdict(new_kbps, old_kbps)

    if ir.decision == "transcode_downgrade":
        return _comparison_verdict(new_kbps, old_kbps, prefix="Transcode at")

    if ir.error:
        return f"Import error: {ir.error}"

    if ir.decision:
        return ir.decision.replace("_", " ")

    return None


def _classify_transcode(entry: LogEntry) -> tuple[str, str, str, str]:
    """Classify a transcode_upgrade or transcode_first success."""
    br = _real_bitrate_kbps(entry)
    br_str = f"{br}kbps" if br else "unknown bitrate"
    if entry.beets_scenario == "transcode_upgrade":
        ex = entry.existing_min_bitrate or entry.existing_spectral_bitrate
        ex_str = f" from {ex}kbps" if ex else ""
        verdict = f"Transcode at {br_str} — imported as upgrade{ex_str}, searching for better"
    else:
        verdict = f"Transcode at {br_str} — imported (nothing on disk), searching for better"
    return ("Transcode", "badge-transcode", "#a93", verdict)


def _classify_search_filetype_override(
    entry: LogEntry,
    is_verified_lossless: bool,
) -> tuple[str, str, str, str]:
    """Classify a search_filetype_override upgrade (replacing unverified CBR)."""
    fmt = entry.actual_filetype or entry.filetype or "mp3"
    cur_label = quality_label(fmt, entry.actual_min_bitrate
                              or entry.request_min_bitrate or 0)
    parts = [f"Replaced unverified CBR with {cur_label}"]
    if entry.was_converted and entry.original_filetype:
        parts.append(f"from {entry.original_filetype.upper()}")
    if is_verified_lossless:
        parts.append("verified lossless")
    return ("Upgraded", "badge-upgraded", "#3a6", ", ".join(parts))


def _new_import_verdict(entry: LogEntry, is_verified_lossless: bool) -> str:
    """Build verdict for a new import (nothing on disk before)."""
    br = entry.actual_min_bitrate or entry.request_min_bitrate
    fmt = entry.actual_filetype or entry.filetype or "mp3"
    label = quality_label(fmt, br or 0)
    parts = [label]
    if entry.was_converted and entry.original_filetype:
        parts.append(f"from {entry.original_filetype.upper()}")
    if is_verified_lossless:
        parts.append("verified lossless")
    return " - ".join(parts) if len(parts) > 1 else parts[0]


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------

def _rejection_verdict(entry: LogEntry) -> str:
    """Build human-readable verdict for a rejected entry.

    For quality comparisons (downgrade, transcode_downgrade), prefer the
    ImportResult JSONB which has accurate measurements. Fall back to
    LogEntry fields only when JSONB is unavailable — and never use
    spectral_bitrate as a proxy for actual file bitrate.
    """
    scenario = entry.beets_scenario

    # Quality comparison scenarios — delegate to ImportResult when available
    if scenario in ("quality_downgrade", "transcode_downgrade"):
        ir_verdict = _quality_verdict_from_import_result(entry)
        if ir_verdict is not None:
            return ir_verdict
        # Fallback: use real file bitrate, not spectral
        new_kbps = _real_bitrate_kbps(entry)
        old_kbps = entry.existing_min_bitrate or entry.existing_spectral_bitrate
        if scenario == "transcode_downgrade":
            return _comparison_verdict(new_kbps, old_kbps, prefix="Transcode at")
        return _comparison_verdict(new_kbps, old_kbps)

    if scenario == "spectral_reject":
        # Spectral scenario — spectral_bitrate IS the right field here
        old_kbps = entry.existing_spectral_bitrate or entry.existing_min_bitrate
        return _comparison_verdict(
            entry.spectral_bitrate, old_kbps, prefix="Spectral:")

    if scenario == "high_distance":
        dist = (f"{float(entry.beets_distance):.3f}"
                if entry.beets_distance is not None else "?")
        return f"Wrong match (dist {dist})"

    if scenario == "audio_corrupt":
        return "Corrupt audio files detected"

    if scenario == "no_candidates":
        return "No MusicBrainz match found"

    if scenario == "album_name_mismatch":
        return "Album name mismatch"

    if scenario == "nested_layout":
        return "Nested folder layout (flatten first)"

    return str(scenario) if scenario else "Rejected"


def _upgrade_verdict(prev_br: Optional[int], cur_br: Optional[int],
                     was_converted: bool, original_ft: Optional[str],
                     is_verified_lossless: bool,
                     actual_filetype: Optional[str] = None) -> str:
    """Build verdict for a successful upgrade."""
    fmt = actual_filetype or "mp3"
    prev_label = quality_label("mp3", prev_br) if prev_br else "?"
    cur_label = quality_label(fmt, cur_br) if cur_br else "?"
    parts = [f"{prev_label} to {cur_label}"]
    if was_converted and original_ft:
        parts.append(f"from {original_ft.upper()}")
    if is_verified_lossless:
        parts.append("verified lossless")
    return "Upgrade: " + ", ".join(parts)


# ---------------------------------------------------------------------------
# Summary (folded in from build_summary_line)
# ---------------------------------------------------------------------------

def _build_summary(entry: LogEntry, badge: str, verdict: str) -> str:
    """Build a one-line summary for the collapsed card view.

    Returns a plain text string (no HTML).
    """
    parts: list[str] = []

    if badge == "Imported":
        # Show format label for new imports
        br = entry.actual_min_bitrate or entry.request_min_bitrate
        fmt = entry.actual_filetype or entry.filetype or "mp3"
        label = quality_label(fmt, br or 0)
        if entry.was_converted and entry.original_filetype:
            label += f" from {entry.original_filetype.upper()}"
        parts.append(label)
    else:
        parts.append(verdict)

    if entry.soulseek_username:
        parts.append(entry.soulseek_username)

    return " \u00b7 ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Downloaded label — server-computed quality description of the download
# ---------------------------------------------------------------------------

def _build_downloaded_label(entry: LogEntry) -> str:
    """Build a label describing what was downloaded.

    Examples: "MP3 320", "FLAC (converted to MP3 V0)", "MP3 V2"
    """
    fmt = entry.actual_filetype or entry.filetype or ""
    if not fmt:
        return ""

    br_kbps = (entry.actual_min_bitrate
               or (entry.bitrate // 1000 if entry.bitrate else None)
               or 0)

    if entry.was_converted and entry.original_filetype:
        conv_label = quality_label(fmt, br_kbps)
        return f"{entry.original_filetype.upper()} (converted to {conv_label})"

    return quality_label(fmt, br_kbps) if br_kbps else fmt.upper()
