"""Pure classification functions for recents tab display.

Given a download_log row (as a LogEntry dataclass), computes a
ClassifiedEntry with badge, verdict, and summary.

No I/O, no database — fully unit-testable.
"""

import json
from dataclasses import dataclass, fields
from typing import Any, Optional

import msgspec

from lib.quality import (AudioQualityMeasurement, ImportResult,
                         QualityComparisonBasis)
from lib.validation_envelope import decode_validation_envelope

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
    # Per-file failure detail audit blob (issue #564 C7, migration 043) —
    # a list of FileFailureDetail dicts behind a download-timeout row's
    # composed error_message summary. Not currently rendered; audit-only.
    transfer_detail: Optional[Any] = None

    # download quality
    filetype: Optional[str] = None
    bitrate: Optional[int] = None              # bps — the ONLY field in bps
    was_converted: bool = False
    original_filetype: Optional[str] = None
    actual_filetype: Optional[str] = None
    actual_min_bitrate: Optional[int] = None   # kbps
    slskd_filetype: Optional[str] = None
    spectral_grade: Optional[str] = None
    spectral_bitrate: Optional[int] = None     # kbps
    existing_min_bitrate: Optional[int] = None  # kbps
    existing_spectral_bitrate: Optional[int] = None  # kbps
    existing_spectral_grade: Optional[str] = None
    final_format: Optional[str] = None
    v0_probe_kind: Optional[str] = None
    v0_probe_min_bitrate: Optional[int] = None
    v0_probe_avg_bitrate: Optional[int] = None
    v0_probe_median_bitrate: Optional[int] = None
    existing_v0_probe_kind: Optional[str] = None
    existing_v0_probe_min_bitrate: Optional[int] = None
    existing_v0_probe_avg_bitrate: Optional[int] = None
    existing_v0_probe_median_bitrate: Optional[int] = None

    # album_requests columns (from JOIN — empty for history-only queries)
    album_title: str = ""
    artist_name: str = ""
    mb_release_id: Optional[str] = None
    request_status: Optional[str] = None
    request_min_bitrate: Optional[int] = None  # kbps
    search_filetype_override: Optional[str] = None
    source: Optional[str] = None
    request_source: Optional[str] = None
    youtube_metadata: Optional[dict[str, Any]] = None

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


class ClassifiedEntry(msgspec.Struct):
    """Classification result for a LogEntry — badge, verdict, and summary."""
    badge: str
    badge_class: str
    border_color: str
    verdict: str
    summary: str
    downloaded_label: str = ""  # e.g. "MP3 320", "FLAC (converted to MP3 V0)"
    # Issue #130: ``PostflightInfo.disambiguation_failure`` reaches JSONB but
    # had no UI surface until this field was added. ``None`` = no failure
    # (either disambiguation succeeded or wasn't attempted); string values
    # mirror ``BeetsOpFailureReason`` Literal: "timeout" | "nonzero_rc" |
    # "exception". ``disambiguation_detail`` carries the short human-readable
    # ``detail`` for hover/tooltip — do not parse it.
    disambiguation_failure: Optional[str] = None
    disambiguation_detail: Optional[str] = None
    bad_extensions: list[str] = msgspec.field(default_factory=list)
    wrong_match_triage_action: Optional[str] = None
    wrong_match_triage_summary: Optional[str] = None
    wrong_match_triage_reason: Optional[str] = None
    wrong_match_triage_preview_verdict: Optional[str] = None
    wrong_match_triage_preview_decision: Optional[str] = None
    wrong_match_triage_stage_chain: list[str] = msgspec.field(default_factory=list)
    wrong_match_triage_detail: Optional[str] = None
    # The on-disk codec at download time, from import_result JSONB
    # (existing_measurement.format). Rank-driven upgrades at equal
    # bitrate are unreadable without it (issue #575: AAC 256 replacing
    # unverified MP3 256 rendered as "256kbps (was 256kbps)").
    existing_format: Optional[str] = None
    # The persisted QualityComparisonBasis as JSON-plain builtins, for the
    # frontend evidence strip / detail grid. None on rows predating the
    # field (request 6039 lesson: labels re-derived from min bitrate lie).
    comparison_basis: dict[str, object] | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    existing_spectral_bitrate: int | None = None
    spectral_attempted: bool | None = None
    spectral_error: str | None = None
    existing_spectral_attempted: bool | None = None
    existing_spectral_error: str | None = None


class _Classification(msgspec.Struct, frozen=True):
    """Typed core outcome used to finish a ``ClassifiedEntry``."""

    badge: str
    badge_class: str
    border_color: str
    verdict: str


# ---------------------------------------------------------------------------
# Quality label
# ---------------------------------------------------------------------------

def _quality_label_from_bitrate(fmt: str, bitrate_kbps: int) -> str:
    """Human-readable codec label from one context-selected bitrate.

    Examples: "MP3 V0", "MP3 320", "FLAC", "MP3 197k".
    """
    if not fmt:
        return "?"
    fmt = fmt.strip().split(",")[0].strip().upper()
    if fmt in ("FLAC", "ALAC"):
        return fmt
    if not bitrate_kbps or bitrate_kbps <= 0:
        return fmt
    if bitrate_kbps >= 295:
        return f"{fmt} 320"
    if bitrate_kbps >= 220:
        return f"{fmt} V0"
    if bitrate_kbps >= 170:
        return f"{fmt} V2"
    return f"{fmt} {bitrate_kbps}k"


def average_quality_label(fmt: str, avg_bitrate_kbps: int) -> str:
    """Current-state label from the average positive track bitrate."""
    return _quality_label_from_bitrate(fmt, avg_bitrate_kbps)


def legacy_floor_quality_label(fmt: str, min_bitrate_kbps: int) -> str:
    """Frozen history label derived from a legacy minimum-track floor.

    Rows without ``comparison_basis`` must retain this vocabulary byte for
    byte. Current-state surfaces must use ``average_quality_label`` instead.
    """
    return _quality_label_from_bitrate(fmt, min_bitrate_kbps)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_log_entry(entry: LogEntry) -> ClassifiedEntry:
    """Classify a download_log entry for display.

    Returns a ClassifiedEntry with badge, verdict, summary, and downloaded_label.
    """
    core = _classify(entry)
    summary = _build_summary(entry, core.badge, core.verdict)
    downloaded_label = _build_downloaded_label(entry)
    existing_format = _extract_existing_format(entry)
    disambig_reason, disambig_detail = _extract_disambiguation_failure(entry)
    bad_extensions = _extract_bad_extensions(entry)
    triage = _extract_wrong_match_triage(entry)
    basis = _entry_comparison_basis(entry)
    spectral = _extract_attempt_spectral(entry)
    return ClassifiedEntry(
        badge=core.badge, badge_class=core.badge_class,
        border_color=core.border_color, verdict=core.verdict,
        summary=summary, downloaded_label=downloaded_label,
        comparison_basis=(
            msgspec.to_builtins(basis) if basis is not None else None
        ),
        disambiguation_failure=disambig_reason,
        disambiguation_detail=disambig_detail,
        bad_extensions=bad_extensions,
        wrong_match_triage_action=triage["action"],
        wrong_match_triage_summary=triage["summary"],
        wrong_match_triage_reason=triage["reason"],
        wrong_match_triage_preview_verdict=triage["preview_verdict"],
        wrong_match_triage_preview_decision=triage["preview_decision"],
        wrong_match_triage_stage_chain=triage["stage_chain"],
        wrong_match_triage_detail=triage["detail"],
        existing_format=existing_format,
        spectral_grade=spectral[0],
        spectral_bitrate=spectral[1],
        existing_spectral_grade=spectral[2],
        existing_spectral_bitrate=spectral[3],
        spectral_attempted=spectral[4],
        spectral_error=spectral[5],
        existing_spectral_attempted=spectral[6],
        existing_spectral_error=spectral[7],
    )


def _extract_attempt_spectral(
    entry: LogEntry,
) -> tuple[
    str | None, int | None, str | None, int | None,
    bool | None, str | None, bool | None, str | None,
]:
    """Prefer attempt-local audit, preserving honest historical fallbacks."""
    ir = _parse_import_result(entry)
    candidate_grade = entry.spectral_grade
    candidate_bitrate = entry.spectral_bitrate
    existing_grade = entry.existing_spectral_grade
    existing_bitrate = entry.existing_spectral_bitrate
    candidate_attempted: bool | None = None
    candidate_error: str | None = None
    existing_attempted: bool | None = None
    existing_error: str | None = None
    if ir is not None:
        candidate = ir.spectral.candidate
        existing = ir.spectral.existing
        if candidate is not None:
            candidate_attempted = candidate.attempted
            candidate_error = candidate.error
            candidate_grade = candidate.grade
            candidate_bitrate = candidate.bitrate_kbps
        if existing is not None:
            existing_attempted = existing.attempted
            existing_error = existing.error
            existing_grade = existing.grade
            existing_bitrate = existing.bitrate_kbps
    return (
        candidate_grade, candidate_bitrate, existing_grade, existing_bitrate,
        candidate_attempted, candidate_error, existing_attempted, existing_error,
    )


def _extract_existing_format(entry: LogEntry) -> Optional[str]:
    """The on-disk codec at download time, from
    ``import_result.existing_measurement.format``. None when the blob is
    missing/legacy — renderers fall back to the bare bitrate suffix."""
    ir = _parse_import_result(entry)
    if ir is None or ir.existing_measurement is None:
        return None
    return ir.existing_measurement.format


def _extract_disambiguation_failure(
    entry: LogEntry,
) -> tuple[Optional[str], Optional[str]]:
    """Pull ``postflight.disambiguation_failure.{reason, detail}`` out of the
    ImportResult JSONB. Returns ``(None, None)`` when no failure is present
    or the blob is missing/unreadable — callers render the chip conditionally.
    """
    ir = _parse_import_result(entry)
    if ir is None or ir.postflight is None:
        return (None, None)
    fail = ir.postflight.disambiguation_failure
    if fail is None:
        return (None, None)
    return (fail.reason, fail.detail)


def _extract_bad_extensions(entry: LogEntry) -> list[str]:
    """Pull postflight bad-extension filenames out of ImportResult JSONB."""
    ir = _parse_import_result(entry)
    if ir is None or ir.postflight is None:
        return []
    return list(ir.postflight.bad_extensions)


def _empty_wrong_match_triage() -> dict[str, Any]:
    return {
        "action": None,
        "summary": None,
        "reason": None,
        "preview_verdict": None,
        "preview_decision": None,
        "stage_chain": [],
        "detail": None,
    }


def _humanize_token(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("_", " ").replace("-", " ").strip()


def _stage_failure_family(stage_chain: list[str]) -> str | None:
    text = " ".join(stage_chain).lower()
    if not text:
        return None
    if "spectral" in text:
        return "spectral"
    if any(token in text for token in ("preimport", "audio", "nested")):
        return "preimport"
    if any(token in text for token in ("quality", "downgrade", "requeue")):
        return "quality"
    if "post" in text and "gate" in text:
        return "post-import"
    return None


def _wrong_match_action_label(action: str | None) -> str | None:
    if action == "deleted_reject":
        return "deleted"
    if action == "deleted_verified_lossless_parent":
        return "deleted: verified-lossless parent"
    if action == "delete_failed":
        return "delete failed"
    if action == "stale_path_cleared":
        return "stale path cleared"
    if action == "stale_path_clear_failed":
        return "stale path clear failed"
    if action == "kept_would_import":
        return "kept: would import"
    if action == "kept_uncertain":
        return "kept: uncertain"
    if action == "preview_backfilled":
        return "previewed"
    return _humanize_token(action)


def _build_wrong_match_triage_summary(
    action: str | None,
    reason: str | None,
    preview_verdict: str | None,
    preview_decision: str | None,
    stage_chain: list[str],
) -> str | None:
    if not action and not reason and not preview_verdict and not preview_decision:
        return None

    family = _stage_failure_family(stage_chain)
    label = _wrong_match_action_label(action)

    if action == "deleted_reject":
        if family:
            return f"deleted: {family} reject"
        detail = (_humanize_token(reason)
                  or _humanize_token(preview_decision)
                  or _humanize_token(preview_verdict))
        return f"deleted: {detail}" if detail else "deleted"

    if action == "deleted_verified_lossless_parent":
        return "deleted: verified-lossless parent in library"

    if action == "kept_would_import":
        return "kept: would import"

    if action == "kept_uncertain":
        detail = (_humanize_token(reason)
                  or _humanize_token(preview_decision)
                  or _humanize_token(preview_verdict))
        return f"kept: {detail}" if detail else "kept: uncertain"

    if label:
        detail = (_humanize_token(reason)
                  or _humanize_token(preview_decision)
                  or _humanize_token(preview_verdict))
        if detail and detail not in label:
            return f"{label}: {detail}"
        return label

    return (_humanize_token(reason)
            or _humanize_token(preview_decision)
            or _humanize_token(preview_verdict))


def _build_wrong_match_triage_detail(
    action: str | None,
    reason: str | None,
    preview_verdict: str | None,
    preview_decision: str | None,
    stage_chain: list[str],
) -> str | None:
    parts: list[str] = []
    if action:
        parts.append(f"action: {_humanize_token(action)}")
    if preview_verdict:
        parts.append(f"verdict: {_humanize_token(preview_verdict)}")
    if preview_decision:
        parts.append(f"decision: {_humanize_token(preview_decision)}")
    if reason:
        parts.append(f"reason: {_humanize_token(reason)}")
    if stage_chain:
        parts.append("stages: " + " · ".join(stage_chain))
    return " · ".join(parts) if parts else None


def _extract_wrong_match_triage(entry: LogEntry) -> dict[str, Any]:
    """Pull preview-driven wrong-match triage audit out of ValidationResult."""
    # Render path: one malformed historical audit row must not 500 the
    # whole recents page (contract pinned by TestClassifyWrongMatchTriageAudit
    # ``*_does_not_raise`` tests). The envelope decode stays strict; the
    # fail-open lives here, at the display boundary only.
    try:
        triage = decode_validation_envelope(
            entry.validation_result).wrong_match_triage
    except (msgspec.ValidationError, json.JSONDecodeError):
        return _empty_wrong_match_triage()
    if triage is None:
        return _empty_wrong_match_triage()

    summary = _build_wrong_match_triage_summary(
        triage.action,
        triage.reason,
        triage.preview_verdict,
        triage.preview_decision,
        triage.stage_chain,
    )
    detail = _build_wrong_match_triage_detail(
        triage.action,
        triage.reason,
        triage.preview_verdict,
        triage.preview_decision,
        triage.stage_chain,
    )
    return {
        "action": triage.action,
        "summary": summary,
        "reason": triage.reason,
        "preview_verdict": triage.preview_verdict,
        "preview_decision": triage.preview_decision,
        "stage_chain": triage.stage_chain,
        "detail": detail,
    }


def _classify(entry: LogEntry) -> _Classification:
    """Build the typed core classification for one log entry."""

    # --- Rejected ---
    if entry.outcome == "rejected":
        verdict = _rejection_verdict(entry)
        return _Classification("Rejected", "badge-rejected", "#a33", verdict)

    # --- Timeout (download-phase; outcome="timeout" is written ONLY by
    # lib/download.py::_timeout_album — error_message is the real
    # per-file evidence summary, issue #564 C5, when any was captured) ---
    if entry.outcome == "timeout":
        verdict = (
            f"Download failed: {entry.error_message}"
            if entry.error_message
            else "Download failed"
        )
        return _Classification("Failed", "badge-failed", "#a33", verdict)

    # --- Failed (import-phase) ---
    if entry.outcome == "failed":
        if entry.beets_scenario == "timeout":
            verdict = "Import timed out"
        elif entry.error_message:
            verdict = f"Import error: {entry.error_message}"
        else:
            verdict = _quality_verdict_from_import_result(entry) or "Import error"
        return _Classification("Failed", "badge-failed", "#a33", verdict)

    # --- Force import ---
    if entry.outcome == "force_import":
        return _Classification(
            "Force imported", "badge-force", "#46a",
            "Force imported after manual review",
        )

    # --- Curator ban (#188 follow-up: bad-rip click is just another event) ---
    if entry.outcome == "curator_ban":
        # validation_result JSONB carries hashes_recorded and the banned
        # username; surface a terse human-readable verdict here. Detail
        # already lives in beets_detail (e.g. "Marked bad rip; 12 hashes
        # captured") which the row renderer can display directly.
        verdict = "Marked bad rip"
        if entry.soulseek_username:
            verdict = f"Marked bad rip — denylisted {entry.soulseek_username}"
        return _Classification("Bad rip", "badge-rejected", "#a33", verdict)

    # --- Peer offline at enqueue (verified rejection written by
    # lib/enqueue.py; issue #564 — previously fell through to the
    # generic "Unknown outcome" branch below) ---
    if entry.outcome == "user_offline":
        verdict = entry.error_message or "Peer offline at enqueue"
        return _Classification(
            "Peer offline", "badge-rejected", "#a33", verdict,
        )

    # --- Success ---
    if entry.outcome == "success":
        if _entry_decision(entry) == "provisional_lossless_upgrade":
            return _classify_provisional(entry)

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
            basis = _entry_comparison_basis(entry)
            if basis is not None:
                verdict = _upgrade_verdict_from_basis(
                    basis, entry.was_converted, entry.original_filetype,
                    is_verified_lossless)
            else:
                verdict = _upgrade_verdict(
                    entry.existing_min_bitrate,
                    _downloaded_min_bitrate_kbps(entry),
                    entry.was_converted, entry.original_filetype,
                    is_verified_lossless,
                    actual_filetype=entry.actual_filetype)
            return _Classification(
                "Upgraded", "badge-upgraded", "#3a6", verdict,
            )

        # New import
        verdict = _new_import_verdict(entry, is_verified_lossless)
        return _Classification("Imported", "badge-new", "#1a4a2a", verdict)

    # --- Unknown outcome --- (humanize: raw enum values like
    # "measurement_failed" must not leak underscores into a badge)
    label = (
        str(entry.outcome).replace("_", " ").capitalize()
        if entry.outcome else "Unknown"
    )
    return _Classification(
        label,
        "badge-rejected",
        "#444",
        str(entry.outcome or "Unknown outcome"),
    )


def _parse_import_result(entry: LogEntry) -> ImportResult | None:
    """Parse the import_result JSONB from a LogEntry, or None.

    Returns None on any decode failure, INCLUDING
    ``msgspec.ValidationError`` from strict-typed decode post-#141:
    historical JSONB rows predate the current schema and the Recents
    tab degrades gracefully to "no typed result" rather than 500ing
    the route on one bad legacy row.
    """
    raw = entry.import_result
    if raw is None:
        return None
    try:
        if isinstance(raw, dict):
            return ImportResult.from_dict(raw)
        elif isinstance(raw, str):
            return ImportResult.from_json(raw)
        return None
    except (json.JSONDecodeError, TypeError, KeyError,
            msgspec.ValidationError):
        return None


def _entry_decision(entry: LogEntry) -> str | None:
    ir = _parse_import_result(entry)
    if ir is not None and ir.decision:
        return ir.decision
    return entry.beets_scenario


def _entry_comparison_basis(entry: LogEntry) -> QualityComparisonBasis | None:
    ir = _parse_import_result(entry)
    if ir is None:
        return None
    return ir.comparison_basis


def _basis_value_phrase(metric: str, value: int | None, clamped: bool) -> str:
    if value is None:
        return "unmeasured"
    if clamped:
        # min(selected metric, spectral floor) — the metric label would lie
        return f"~{value}k"
    return f"{metric} {value}k"


def _verdict_from_basis(basis: QualityComparisonBasis) -> str:
    """Render the persisted comparison exactly as the decider performed it.

    This is the whole point of QualityComparisonBasis (request 6039): the
    verdict names the branch that fired, the per-side stat actually
    classified, and the ranks — never re-derived from row columns. Rows
    without a basis keep the legacy min-based rendering elsewhere.
    """
    new_fmt = (basis.new_format or "?").upper()
    ex_fmt = (basis.existing_format or "?").upper()
    clamped = basis.spectral_clamped and basis.branch == "rank"
    new_val = _basis_value_phrase(basis.new_metric, basis.new_value_kbps, clamped)
    ex_val = _basis_value_phrase(
        basis.existing_metric, basis.existing_value_kbps, clamped)

    if basis.verdict == "better":
        if basis.branch == "metric_tiebreak":
            return (f"Upgrade: {ex_fmt} {ex_val} → {new_val} "
                    f"(both {basis.new_rank})")
        new_side = new_val if new_fmt == ex_fmt else f"{new_fmt} {new_val}"
        return (f"Upgrade: {ex_fmt} {ex_val} ({basis.existing_rank}) → "
                f"{new_side} ({basis.new_rank})")

    if basis.verdict == "worse":
        prefix = ("Transcode-grade: "
                  if basis.branch == "transcode_rank_regression" else "")
        ex_side = ex_val if new_fmt == ex_fmt else f"{ex_fmt} {ex_val}"
        return (f"{prefix}{new_fmt} {new_val} ({basis.new_rank}) — "
                f"not better than existing {ex_side} ({basis.existing_rank})")

    # equivalent — the branch is the story
    if basis.branch == "lossless_same_rank":
        core = "both lossless"
    elif basis.branch == "cross_family_same_rank":
        core = f"{new_fmt} vs {ex_fmt} — both {basis.new_rank}"
    elif basis.branch == "label_contract_same_rank":
        core = f"{new_fmt} vs {ex_fmt} — label contract, both {basis.new_rank}"
    elif basis.branch == "metric_missing":
        core = "bitrate unmeasurable"
    else:  # metric_tiebreak
        tol = (f" (within {basis.tolerance_kbps}k)"
               if basis.tolerance_kbps is not None else "")
        core = f"{new_fmt} {new_val} vs {ex_val}{tol}"
    verdict = f"Equivalent: {core}"
    if basis.verified_lossless_bypass:
        verdict += " — imported: verified lossless"
    return verdict


def _upgrade_verdict_from_basis(
    basis: QualityComparisonBasis,
    was_converted: bool,
    original_ft: Optional[str],
    is_verified_lossless: bool,
) -> str:
    """Basis-driven twin of _upgrade_verdict, keeping the legacy suffixes."""
    parts = [_verdict_from_basis(basis)]
    if was_converted and original_ft:
        parts.append(f"from {original_ft.upper()}")
    if is_verified_lossless and not basis.verified_lossless_bypass:
        parts.append("verified lossless")
    return ", ".join(parts)


def _downloaded_min_bitrate_kbps(entry: LogEntry) -> int | None:
    """The min bitrate of the file that was downloaded for THIS log row.

    Point-in-time — reflects this download's state, not the album's current
    state. Callers MUST NOT fall back to ``entry.request_min_bitrate`` for
    per-row displays: request_min_bitrate is ``album_requests.min_bitrate``
    at query time (the album's current state), so after a subsequent upgrade
    it no longer matches what this row imported. Using it to paint the 'to'
    bitrate in an older Recents card invents a fake self-upgrade (see live
    reproducer request 1055: brandlos's 119k import was painted as 162k
    because that's Ceezles's later upgrade).

    Priority chain:
        1. ``entry.actual_min_bitrate`` — denormalized column, populated by
           ``_populate_dl_info_from_import_result`` on the auto-import path
           since the ``actual_min_bitrate`` fix.
        2. ``ir.new_measurement.min_bitrate_kbps`` — authoritative JSONB,
           present on every row. Fixes historical rows (pre-column-fix)
           retroactively without a backfill migration.
        3. ``entry.bitrate`` (bps) — legacy container bitrate, last resort.

    ``spectral_bitrate`` is a cliff estimate ("what was the original source?"),
    not the file's actual bitrate. It must never appear here.
    """
    if entry.actual_min_bitrate:
        return entry.actual_min_bitrate
    ir = _parse_import_result(entry)
    if ir is not None and ir.new_measurement is not None:
        if ir.new_measurement.min_bitrate_kbps is not None:
            return ir.new_measurement.min_bitrate_kbps
    if entry.bitrate:
        return entry.bitrate // 1000
    return None


def _comparison_verdict(
    new_kbps: int | None,
    old_kbps: int | None,
    prefix: str = "",
    metric: str | None = None,
    new_spectral_kbps: int | None = None,
    old_spectral_kbps: int | None = None,
    new_spectral_grade: str | None = None,
    old_spectral_grade: str | None = None,
) -> str:
    """Build a '… is not better than existing …' verdict string.

    ``metric`` ("avg" / "min") annotates the bitrates so the reader can see
    which number drove the rank comparison. This matters when the backend's
    spectral override clamped ``min`` but kept ``avg`` on a VBR existing —
    without the label, a verdict like "152 is not better than 96" reads as
    a contradiction (it's really "avg 152 is not better than avg 225" but
    min was shown and looked inverted). Pass ``None`` to omit the label when
    both sides come from the same raw min.
    """
    new_s = _measurement_phrase(
        new_kbps,
        metric,
        spectral_kbps=new_spectral_kbps,
        spectral_grade=new_spectral_grade,
    )
    old_s = _measurement_phrase(
        old_kbps,
        metric,
        spectral_kbps=old_spectral_kbps,
        spectral_grade=old_spectral_grade,
    )
    if prefix:
        return f"{prefix} {new_s} — not better than existing {old_s}"
    return f"{new_s} is not better than existing {old_s}"


def _measurement_spectral_phrase(
    spectral_kbps: int | None,
    spectral_grade: str | None,
) -> str | None:
    if spectral_kbps is None and not spectral_grade:
        return None
    if spectral_grade and spectral_kbps is not None:
        return f"spectral {spectral_grade} ~{spectral_kbps}kbps"
    if spectral_grade:
        return f"spectral {spectral_grade}"
    return f"spectral ~{spectral_kbps}kbps"


def _measurement_phrase(
    bitrate_kbps: int | None,
    metric: str | None,
    *,
    spectral_kbps: int | None = None,
    spectral_grade: str | None = None,
) -> str:
    suffix = f" {metric}" if metric else ""
    primary = (
        f"{bitrate_kbps}kbps{suffix}"
        if bitrate_kbps is not None
        else "unknown"
    )
    spectral = _measurement_spectral_phrase(spectral_kbps, spectral_grade)
    if spectral:
        return f"{primary} ({spectral})"
    return primary


def _verdict_bitrate(
    m: AudioQualityMeasurement | None,
) -> tuple[int | None, str | None]:
    """Pick the bitrate and metric label to display for a measurement.

    Prefers ``avg_bitrate_kbps`` when present (production's default
    ``cfg.bitrate_metric=avg``) and falls back to ``min_bitrate_kbps``,
    then ``spectral_bitrate_kbps`` as a last resort. Returns the value
    plus a metric label ("avg" / "min" / None) so the caller can annotate
    the verdict string.

    Returning avg when it exists is important after the 2026-04-21
    CBR-conditional override fix: for VBR existing, ``avg_bitrate_kbps``
    carries the real signal that drove the rank comparison while
    ``min_bitrate_kbps`` has been clamped by the spectral override.
    Displaying min alone produced contradictory-looking verdicts
    ("new 152 is not better than existing 96"). Spectral is rendered as
    separate context by ``_comparison_verdict`` so the selected real bitrate
    and source-quality estimate remain visible together.
    """
    if m is None:
        return None, None
    if m.avg_bitrate_kbps is not None:
        return m.avg_bitrate_kbps, "avg"
    if m.min_bitrate_kbps is not None:
        return m.min_bitrate_kbps, "min"
    if m.spectral_bitrate_kbps is not None:
        return m.spectral_bitrate_kbps, None
    return None, None


def _quality_verdict_from_import_result(entry: LogEntry) -> str | None:
    """Derive a quality comparison verdict from ImportResult JSONB.

    Used by both rejected and failed outcomes — single source of truth
    for "X is not better than Y" messages.
    """
    ir = _parse_import_result(entry)
    if ir is None:
        return None

    # The persisted basis is the decision's own story — render it verbatim
    # instead of re-deriving a comparison from the measurements (request
    # 6039: re-derivation is how the display learned to lie).
    if (ir.comparison_basis is not None
            and ir.decision in ("downgrade", "transcode_downgrade")):
        verdict = _verdict_from_basis(ir.comparison_basis)
        if ir.decision == "transcode_downgrade":
            return f"Transcode — {verdict}"
        return verdict

    new_m = ir.new_measurement
    existing_m = ir.existing_measurement
    new_kbps, new_metric = _verdict_bitrate(new_m)
    old_kbps, old_metric = _verdict_bitrate(existing_m)
    # Prefer the richer metric label when sides disagree ("avg" > "min")
    # so the string doesn't read "new 152kbps avg vs existing 96kbps min".
    # That split is rare — both sides come from the same ImportResult —
    # but falling back to None keeps the verdict readable in the edge case.
    metric = new_metric if new_metric == old_metric else None

    if ir.decision == "downgrade":
        return _comparison_verdict(
            new_kbps, old_kbps, metric=metric,
            new_spectral_kbps=(
                new_m.spectral_bitrate_kbps if new_m is not None else None
            ),
            old_spectral_kbps=(
                existing_m.spectral_bitrate_kbps
                if existing_m is not None else None
            ),
            new_spectral_grade=(
                new_m.spectral_grade if new_m is not None else None
            ),
            old_spectral_grade=(
                existing_m.spectral_grade if existing_m is not None else None
            ),
        )

    if ir.decision == "transcode_downgrade":
        return _comparison_verdict(
            new_kbps, old_kbps, prefix="Transcode at", metric=metric,
            new_spectral_kbps=(
                new_m.spectral_bitrate_kbps if new_m is not None else None
            ),
            old_spectral_kbps=(
                existing_m.spectral_bitrate_kbps
                if existing_m is not None else None
            ),
            new_spectral_grade=(
                new_m.spectral_grade if new_m is not None else None
            ),
            old_spectral_grade=(
                existing_m.spectral_grade if existing_m is not None else None
            ),
        )

    if ir.decision == "suspect_lossless_downgrade":
        return _provisional_verdict(entry, imported=False)

    if ir.decision == "suspect_lossless_probe_missing":
        return _provisional_verdict(entry, imported=False)

    if ir.decision == "lossless_source_locked":
        return _lossless_source_locked_verdict(entry)

    if ir.error:
        return f"Import error: {ir.error}"

    if ir.decision:
        return ir.decision.replace("_", " ")

    return None


def _classify_transcode(entry: LogEntry) -> _Classification:
    """Classify a transcode_upgrade or transcode_first success."""
    br = _downloaded_min_bitrate_kbps(entry)
    br_str = f"{br}kbps" if br else "unknown bitrate"
    if entry.beets_scenario == "transcode_upgrade":
        ex = entry.existing_min_bitrate or entry.existing_spectral_bitrate
        ex_str = f" from {ex}kbps" if ex else ""
        verdict = f"Transcode at {br_str} — imported as upgrade{ex_str}, searching for better"
    else:
        verdict = f"Transcode at {br_str} — imported (nothing on disk), searching for better"
    return _Classification("Transcode", "badge-transcode", "#a93", verdict)


def _probe_values(entry: LogEntry) -> tuple[int | None, int | None, str | None]:
    ir = _parse_import_result(entry)
    candidate_avg = entry.v0_probe_avg_bitrate
    existing_avg = entry.existing_v0_probe_avg_bitrate
    final_format = entry.final_format
    if ir is not None:
        if candidate_avg is None and ir.v0_probe is not None:
            candidate_avg = ir.v0_probe.avg_bitrate_kbps
        if existing_avg is None and ir.existing_v0_probe is not None:
            existing_avg = ir.existing_v0_probe.avg_bitrate_kbps
        if not final_format:
            final_format = ir.final_format
        if not final_format and ir.new_measurement is not None:
            final_format = ir.new_measurement.format
    return candidate_avg, existing_avg, final_format


def _spectral_phrase(entry: LogEntry) -> str | None:
    if not entry.spectral_grade:
        return None
    phrase = f"spectral {entry.spectral_grade}"
    if entry.spectral_bitrate:
        phrase += f" ~{entry.spectral_bitrate}kbps"
    return phrase


def _lossless_source_locked_verdict(entry: LogEntry) -> str:
    """Verdict copy for the lossless-source lock rejection.

    Fires when a lossy candidate is offered against an existing album whose
    original lossless-source V0 probe is already recorded. The candidate
    cannot produce comparable evidence — only another lossless-container
    source can override the recorded probe.
    """
    _, existing_avg, _ = _probe_values(entry)
    new_kbps = _downloaded_min_bitrate_kbps(entry)
    parts: list[str] = []
    if new_kbps is not None:
        spectral = _spectral_phrase(entry)
        new_phrase = f"{new_kbps}kbps lossy candidate"
        if spectral:
            new_phrase += f" ({spectral})"
        parts.append(new_phrase)
    if existing_avg is not None:
        parts.append(
            f"existing has lossless-source V0 probe {existing_avg}kbps"
        )
    parts.append("only another lossless source can override")
    parts.append("searching continues")
    return "Lossless-source locked: " + "; ".join(parts)


def _provisional_verdict(entry: LogEntry, *, imported: bool) -> str:
    candidate_avg, existing_avg, final_format = _probe_values(entry)
    parts: list[str] = []
    spectral = _spectral_phrase(entry)
    if spectral:
        parts.append(spectral)
    if candidate_avg is not None:
        parts.append(f"source V0 avg {candidate_avg}kbps")
    if existing_avg is not None:
        parts.append(f"existing source V0 avg {existing_avg}kbps")
    else:
        parts.append("no comparable source probe")
    if imported:
        if final_format:
            parts.append(f"stored as {final_format}")
        parts.append("source denylisted")
        parts.append("searching continues")
        return "Provisional lossless source: " + "; ".join(parts)
    parts.append("not meaningfully better")
    parts.append("searching continues")
    return "Suspect lossless source rejected: " + "; ".join(parts)


def _classify_provisional(entry: LogEntry) -> _Classification:
    return _Classification(
        "Provisional",
        "badge-provisional",
        "#6a5",
        _provisional_verdict(entry, imported=True),
    )


def _classify_search_filetype_override(
    entry: LogEntry,
    is_verified_lossless: bool,
) -> _Classification:
    """Classify a search_filetype_override upgrade (replacing unverified CBR)."""
    fmt = entry.actual_filetype or entry.filetype or "mp3"
    cur_label = legacy_floor_quality_label(fmt, _downloaded_min_bitrate_kbps(entry) or 0)
    parts = [f"Replaced unverified CBR with {cur_label}"]
    if entry.was_converted and entry.original_filetype:
        parts.append(f"from {entry.original_filetype.upper()}")
    if is_verified_lossless:
        parts.append("verified lossless")
    return _Classification(
        "Upgraded", "badge-upgraded", "#3a6", ", ".join(parts),
    )


def _new_import_verdict(entry: LogEntry, is_verified_lossless: bool) -> str:
    """Build verdict for a new import (nothing on disk before)."""
    br = _downloaded_min_bitrate_kbps(entry)
    fmt = entry.actual_filetype or entry.filetype or "mp3"
    label = legacy_floor_quality_label(fmt, br or 0)
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
    if scenario in (
        "downgrade",
        "quality_downgrade",
        "transcode_downgrade",
        "suspect_lossless_downgrade",
        "suspect_lossless_probe_missing",
        "lossless_source_locked",
    ):
        ir_verdict = _quality_verdict_from_import_result(entry)
        if ir_verdict is not None:
            return ir_verdict
        if scenario.startswith("suspect_lossless"):
            return _provisional_verdict(entry, imported=False)
        if scenario == "lossless_source_locked":
            return _lossless_source_locked_verdict(entry)
        # Fallback: use real file bitrate, not spectral
        new_kbps = _downloaded_min_bitrate_kbps(entry)
        old_kbps = entry.existing_min_bitrate or entry.existing_spectral_bitrate
        if scenario == "transcode_downgrade":
            return _comparison_verdict(
                new_kbps,
                old_kbps,
                prefix="Transcode at",
                new_spectral_kbps=entry.spectral_bitrate,
                old_spectral_kbps=entry.existing_spectral_bitrate,
                new_spectral_grade=entry.spectral_grade,
            )
        return _comparison_verdict(
            new_kbps,
            old_kbps,
            new_spectral_kbps=entry.spectral_bitrate,
            old_spectral_kbps=entry.existing_spectral_bitrate,
            new_spectral_grade=entry.spectral_grade,
        )

    if scenario == "spectral_reject":
        # Spectral scenario — spectral_bitrate IS the right field here
        old_kbps = entry.existing_spectral_bitrate or entry.existing_min_bitrate
        # New preimport_decide path (post 2026-05-16 hotfix #254) writes the
        # bitrate numbers into beets_detail (e.g. "spectral 128kbps <= existing
        # 192kbps") but leaves the typed spectral_bitrate / existing_spectral_*
        # columns NULL on the row, because _route_preimport_decision_reject's
        # synthesized DownloadInfo doesn't carry spectral facts. When the
        # typed columns are absent but beets_detail has a real string, render
        # that directly — the importer already wrote a good human-readable
        # reason.
        if (
            entry.spectral_bitrate is None
            and old_kbps is None
            and entry.beets_detail
        ):
            return f"Spectral: {entry.beets_detail}"
        return _comparison_verdict(
            entry.spectral_bitrate, old_kbps, prefix="Spectral:")

    if scenario == "high_distance":
        dist = (f"{float(entry.beets_distance):.3f}"
                if entry.beets_distance is not None else "?")
        return f"Wrong match (dist {dist})"

    if scenario == "audio_corrupt":
        return "Corrupt audio files detected"

    if scenario == "duplicate_remove_guard_failed":
        ir = _parse_import_result(entry)
        guard = ir.postflight.duplicate_remove_guard if ir is not None else None
        if guard is not None:
            return (
                "Duplicate remove guard failed: "
                f"{guard.message} ({guard.duplicate_count} duplicate"
                f"{'' if guard.duplicate_count == 1 else 's'})"
            )
        if entry.beets_detail:
            return f"Duplicate remove guard failed: {entry.beets_detail}"
        return "Duplicate remove guard failed"

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
    prev_label = legacy_floor_quality_label("mp3", prev_br) if prev_br else "?"
    cur_label = legacy_floor_quality_label(fmt, cur_br) if cur_br else "?"
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
        br = _downloaded_min_bitrate_kbps(entry)
        fmt = entry.actual_filetype or entry.filetype or "mp3"
        label = legacy_floor_quality_label(fmt, br or 0)
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

    br_kbps = _downloaded_min_bitrate_kbps(entry) or 0

    if entry.was_converted and entry.original_filetype:
        conv_label = legacy_floor_quality_label(fmt, br_kbps)
        return f"{entry.original_filetype.upper()} (converted to {conv_label})"

    return legacy_floor_quality_label(fmt, br_kbps) if br_kbps else fmt.upper()
