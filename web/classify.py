"""Pure classification functions for recents tab display.

Given a download_log row (as a dict), these functions compute:
- badge + badge_class + border_color (visual classification)
- verdict (human-readable one-liner explaining what happened)
- summary (concise line for the collapsed card view)

No I/O, no database — fully unit-testable.
"""

from typing import Any, Optional


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


def _get(item: dict, key: str, default: Any = None) -> Any:
    """Safe dict get."""
    v = item.get(key)
    return v if v is not None else default


def classify_log_entry(item: dict) -> dict:
    """Classify a download_log entry for display.

    Returns dict with keys: badge, badge_class, border_color, verdict
    """
    outcome = _get(item, "outcome", "")
    scenario = _get(item, "beets_scenario", "")
    distance = _get(item, "beets_distance")
    actual_br = _get(item, "actual_min_bitrate")
    existing_br = _get(item, "existing_min_bitrate")
    spectral_br = _get(item, "spectral_bitrate")
    existing_spectral_br = _get(item, "existing_spectral_bitrate")
    cur_br = _get(item, "request_min_bitrate")
    quality_override = _get(item, "quality_override")
    was_converted = _get(item, "was_converted", False)
    original_ft = _get(item, "original_filetype")
    spectral_grade = _get(item, "spectral_grade")

    # --- Rejected ---
    if outcome == "rejected":
        bitrate_bps = _get(item, "bitrate")
        verdict = _rejection_verdict(scenario, distance, actual_br, existing_br,
                                     spectral_br, existing_spectral_br,
                                     bitrate_bps=bitrate_bps)
        return {
            "badge": "Rejected",
            "badge_class": "badge-rejected",
            "border_color": "#a33",
            "verdict": verdict,
        }

    # --- Failed / Timeout ---
    if outcome in ("failed", "timeout"):
        if scenario == "timeout":
            verdict = "Import timed out"
        else:
            verdict = "Import error"
        return {
            "badge": "Failed",
            "badge_class": "badge-failed",
            "border_color": "#a33",
            "verdict": verdict,
        }

    # --- Force import ---
    if outcome == "force_import":
        return {
            "badge": "Force imported",
            "badge_class": "badge-force",
            "border_color": "#46a",
            "verdict": "Force imported after manual review",
        }

    # --- Success ---
    if outcome == "success":
        # Transcode scenarios
        if scenario in ("transcode_upgrade", "transcode_first"):
            bitrate_bps = _get(item, "bitrate")
            br = actual_br or spectral_br or (int(bitrate_bps) // 1000 if bitrate_bps else None)
            br_str = f"{br}kbps" if br else "unknown bitrate"
            if scenario == "transcode_upgrade":
                ex = existing_br or existing_spectral_br
                ex_str = f" from {ex}kbps" if ex else ""
                verdict = f"Transcode at {br_str} — imported as upgrade{ex_str}, searching for better"
            else:
                verdict = f"Transcode at {br_str} — imported (nothing on disk), searching for better"
            return {
                "badge": "Transcode",
                "badge_class": "badge-transcode",
                "border_color": "#a93",
                "verdict": verdict,
            }

        # Verified lossless upgrade (FLAC→V0 with genuine spectral)
        is_verified_lossless = (was_converted and
                                original_ft and original_ft.lower() == "flac" and
                                spectral_grade == "genuine")

        # Upgrade vs new import — use existing_min_bitrate from the
        # download_log entry (what was on disk at the time of THIS download),
        # NOT prev_min_bitrate from album_requests (current state that
        # changes over time as later downloads update it)
        had_existing = existing_br is not None and existing_br > 0
        if had_existing:
            # Had something on disk — is this an upgrade?
            if quality_override:
                # Replacing unverified CBR with verified source
                cur_label = quality_label("MP3", int(actual_br or cur_br or 0))
                parts = [f"Replaced unverified CBR with {cur_label}"]
                if was_converted and original_ft:
                    parts.append(f"from {original_ft.upper()}")
                if is_verified_lossless:
                    parts.append("verified lossless")
                verdict = ", ".join(parts)
                return {
                    "badge": "Upgraded",
                    "badge_class": "badge-upgraded",
                    "border_color": "#3a6",
                    "verdict": verdict,
                }
            verdict = _upgrade_verdict(existing_br, actual_br or cur_br,
                                       was_converted, original_ft,
                                       is_verified_lossless)
            return {
                "badge": "Upgraded",
                "badge_class": "badge-upgraded",
                "border_color": "#3a6",
                "verdict": verdict,
            }
        else:
            # New import
            br = actual_br or cur_br
            fmt = _get(item, "actual_filetype") or _get(item, "filetype") or "mp3"
            label = quality_label(str(fmt), br or 0)
            parts = [label]
            if was_converted and original_ft:
                parts.append(f"from {original_ft.upper()}")
            if is_verified_lossless:
                parts.append("verified lossless")
            verdict = " - ".join(parts) if len(parts) > 1 else parts[0]
            return {
                "badge": "Imported",
                "badge_class": "badge-new",
                "border_color": "#1a4a2a",
                "verdict": verdict,
            }

    # --- Unknown outcome ---
    return {
        "badge": str(outcome).capitalize() if outcome else "Unknown",
        "badge_class": "badge-rejected",
        "border_color": "#444",
        "verdict": str(outcome or "Unknown outcome"),
    }


def _rejection_verdict(scenario: Any, distance: Any,
                       actual_br: Any, existing_br: Any,
                       spectral_br: Any, existing_spectral_br: Any,
                       bitrate_bps: Any = None) -> str:
    """Build human-readable verdict for a rejected entry.

    Bitrate fields have gaps — actual_min_bitrate is often NULL while
    bitrate (bps) or spectral_bitrate has the value. Fall through all
    available sources.
    """
    # Best available "new" bitrate in kbps
    new_kbps = (actual_br
                or spectral_br
                or (int(bitrate_bps) // 1000 if bitrate_bps else None))
    # Best available "existing" bitrate in kbps
    old_kbps = existing_br or existing_spectral_br

    if scenario == "quality_downgrade":
        new = f"{new_kbps}kbps" if new_kbps else "unknown"
        old = f"{old_kbps}kbps" if old_kbps else "unknown"
        return f"{new} is not better than existing {old}"

    if scenario == "spectral_reject":
        new = f"{spectral_br}kbps" if spectral_br else "unknown"
        old = f"{existing_spectral_br}kbps" if existing_spectral_br else "unknown"
        return f"Spectral: {new} is not better than existing {old}"

    if scenario == "transcode_downgrade":
        new = f"{new_kbps}kbps" if new_kbps else "unknown"
        old = f"{old_kbps}kbps" if old_kbps else "unknown"
        return f"Transcode at {new} — not better than existing {old}"

    if scenario == "high_distance":
        dist = f"{float(distance):.3f}" if distance is not None else "?"
        return f"Wrong match (dist {dist})"

    if scenario == "audio_corrupt":
        return "Corrupt audio files detected"

    if scenario == "no_candidates":
        return "No MusicBrainz match found"

    if scenario == "album_name_mismatch":
        return "Album name mismatch"

    # Fallback for unknown scenarios
    return str(scenario) if scenario else "Rejected"


def _upgrade_verdict(prev_br: Any, cur_br: Any,
                     was_converted: bool, original_ft: Optional[str],
                     is_verified_lossless: bool) -> str:
    """Build verdict for a successful upgrade."""
    prev_label = quality_label("MP3", int(prev_br)) if prev_br else "?"
    cur_label = quality_label("MP3", int(cur_br)) if cur_br else "?"
    parts = [f"{prev_label} to {cur_label}"]
    if was_converted and original_ft:
        parts.append(f"from {original_ft.upper()}")
    if is_verified_lossless:
        parts.append("verified lossless")
    return "Upgrade: " + ", ".join(parts)


def build_summary_line(item: dict, classified: dict) -> str:
    """Build a one-line summary for the collapsed card view.

    Returns a plain text string (no HTML).
    """
    verdict = classified.get("verdict", "")
    username = _get(item, "soulseek_username")
    badge = classified.get("badge", "")

    parts = []

    if badge == "Rejected" or badge == "Failed":
        parts.append(verdict)
    elif badge == "Imported":
        # Show format for new imports
        br = _get(item, "actual_min_bitrate") or _get(item, "request_min_bitrate")
        fmt = _get(item, "actual_filetype") or _get(item, "filetype") or "mp3"
        label = quality_label(str(fmt), int(br) if br else 0)
        was_converted = _get(item, "was_converted", False)
        original_ft = _get(item, "original_filetype")
        if was_converted and original_ft:
            label += f" from {str(original_ft).upper()}"
        parts.append(label)
    elif badge == "Upgraded":
        parts.append(verdict)
    elif badge == "Transcode":
        parts.append(verdict)
    elif badge == "Force imported":
        parts.append(verdict)
    else:
        parts.append(verdict)

    if username:
        parts.append(str(username))

    return " \u00b7 ".join(p for p in parts if p)
