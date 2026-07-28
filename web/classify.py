"""Pure classification functions for recents tab display.

Given a download_log row (as a LogEntry dataclass), computes a
ClassifiedEntry with badge, verdict, and summary.

No I/O, no database — fully unit-testable.
"""

import json
from dataclasses import dataclass, fields
from typing import Any, Self

import msgspec

from lib.failure_presentation import (
    MAX_DIAGNOSTIC_CHARS,
    FailureEvidence,
    FailurePresentation,
    bounded_text,
    decode_transfer_detail,
    present_failure,
    transfer_detail_unreadable,
)
from lib.import_evidence import HaveAnalysisFailure
from lib.import_queue import ImportJob
from lib.json_narrow import is_list_like, json_list
from lib.quality import ImportResult, QualityComparisonBasis, dispatch_action
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
    created_at: str | None = None  # ISO string after serialization

    # match result
    beets_scenario: str | None = None
    beets_distance: float | None = None
    source_download_log_id: int | None = None
    original_beets_distance: float | None = None
    beets_detail: str | None = None
    soulseek_username: str | None = None
    error_message: str | None = None
    download_path: str | None = None
    staged_path: str | None = None
    import_result: dict[str, object] | str | None = None
    validation_result: dict[str, object] | str | None = None
    # Per-file failure detail audit blob (issue #564 C7, migration 043) —
    # a list of FileFailureDetail dicts behind a download-timeout row's
    # composed error_message summary. Read by lib/failure_presentation.py
    # (issue #868): the humane verdict and the bounded raw peer message on
    # the classified entry are BOTH derived from it at render time; this
    # column itself is never rewritten.
    transfer_detail: list[dict[str, object]] | None = None

    # download quality
    filetype: str | None = None
    bitrate: int | None = None              # bps — the ONLY field in bps
    was_converted: bool = False
    original_filetype: str | None = None
    actual_filetype: str | None = None
    actual_min_bitrate: int | None = None   # kbps
    # Immutable candidate-evidence presentation fallback for historical rows
    # without a native import_result/source projection.
    source_format: str | None = None
    source_min_bitrate: int | None = None
    source_avg_bitrate: int | None = None
    source_median_bitrate: int | None = None
    slskd_filetype: str | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None     # kbps
    existing_min_bitrate: int | None = None  # kbps
    existing_spectral_bitrate: int | None = None  # kbps
    existing_spectral_grade: str | None = None
    final_format: str | None = None
    v0_probe_kind: str | None = None
    v0_probe_min_bitrate: int | None = None
    v0_probe_avg_bitrate: int | None = None
    v0_probe_median_bitrate: int | None = None
    existing_v0_probe_kind: str | None = None
    existing_v0_probe_min_bitrate: int | None = None
    existing_v0_probe_avg_bitrate: int | None = None
    existing_v0_probe_median_bitrate: int | None = None

    # album_requests columns (from JOIN — empty for history-only queries)
    album_title: str = ""
    artist_name: str = ""
    mb_release_id: str | None = None
    request_status: str | None = None
    request_min_bitrate: int | None = None  # kbps
    search_filetype_override: str | None = None
    source: str | None = None
    request_source: str | None = None
    youtube_metadata: dict[str, Any] | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
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
    # Candidate measurement as it is safe to render. The raw download-log
    # value remains in the persisted row/import_result; corrupt source bytes
    # deliberately project ``None`` so UI fallbacks cannot paint ``0k``.
    actual_min_bitrate: int | None = None
    # Issue #130: ``PostflightInfo.disambiguation_failure`` reaches JSONB but
    # had no UI surface until this field was added. ``None`` = no failure
    # (either disambiguation succeeded or wasn't attempted); string values
    # mirror ``BeetsOpFailureReason`` Literal: "timeout" | "nonzero_rc" |
    # "exception". ``disambiguation_detail`` carries the short human-readable
    # ``detail`` for hover/tooltip — do not parse it.
    disambiguation_failure: str | None = None
    disambiguation_detail: str | None = None
    bad_extensions: list[str] = msgspec.field(default_factory=list[str])
    wrong_match_triage_action: str | None = None
    wrong_match_triage_summary: str | None = None
    wrong_match_triage_reason: str | None = None
    wrong_match_triage_preview_verdict: str | None = None
    wrong_match_triage_preview_decision: str | None = None
    wrong_match_triage_stage_chain: list[str] = msgspec.field(
        default_factory=list[str],
    )
    wrong_match_triage_detail: str | None = None
    # The on-disk codec at download time, from import_result JSONB
    # (current_measurement.format). Rank-driven upgrades at equal
    # bitrate are unreadable without it (issue #575: AAC 256 replacing
    # unverified MP3 256 rendered as "256kbps (was 256kbps)").
    existing_format: str | None = None
    # v3 lineage facts. Historical v1/v2 rows leave source_* unset because
    # their projected source measurement can be a target-labelled V0 proxy.
    source_format: str | None = None
    source_min_bitrate: int | None = None
    source_avg_bitrate: int | None = None
    source_median_bitrate: int | None = None
    target_contract_format: str | None = None
    legacy_projection_version: int | None = None
    # Post-conversion files measured from Beets postflight. These are distinct
    # from source_measurement (downloaded-source decision input).
    materialized_format: str | None = None
    materialized_min_bitrate: int | None = None
    materialized_avg_bitrate: int | None = None
    materialized_median_bitrate: int | None = None
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
    # The classifier owns the final attempt-local projection for both normal
    # import rows and persisted wrong-match triage snapshots.
    existing_min_bitrate: int | None = None
    existing_avg_bitrate: int | None = None
    existing_median_bitrate: int | None = None
    v0_probe_kind: str | None = None
    v0_probe_min_bitrate: int | None = None
    v0_probe_avg_bitrate: int | None = None
    v0_probe_median_bitrate: int | None = None
    existing_v0_probe_kind: str | None = None
    existing_v0_probe_min_bitrate: int | None = None
    existing_v0_probe_avg_bitrate: int | None = None
    existing_v0_probe_median_bitrate: int | None = None
    # Bounded, deduplicated raw per-transfer text behind a failure verdict
    # (issue #868). ``transfer_detail`` is deliberately excluded from the
    # payload, so without this field humanizing the verdict would delete the
    # peer's own words from the UI entirely. ``transfer_message_label`` is
    # server-owned display copy: "Peer message" when the failures belong to
    # peers, "Storage error" when every one of them was slskd failing to
    # write to OUR share — a local storage fault must never be rendered as
    # something a peer said.
    transfer_message: str | None = None
    transfer_message_label: str | None = None
    # Label for the raw ``beets_detail`` forensics row. PR1 persists the
    # materialize reason there, so the card must not caption a machine
    # token as beets prose (issue #868 review).
    beets_detail_label: str | None = None
    # Environment-failure diagnostics for ``have_analysis_error``. These are
    # intentionally distinct from quality evidence: the installed copy could
    # not be analysed, so the attempt aborted before a quality verdict.
    failure_category: str | None = None
    analysis_error: str | None = None
    installed_path: str | None = None
    candidate_reference: str | None = None


class ImportJobDisplay(msgspec.Struct, frozen=True):
    """Server-owned display contract for one active importer job."""

    badge: str
    badge_class: str
    border_color: str
    summary: str


class _Classification(msgspec.Struct, frozen=True):
    """Typed core outcome used to finish a ``ClassifiedEntry``."""

    badge: str
    badge_class: str
    border_color: str
    verdict: str


class _HaveAnalysisDiagnostics(msgspec.Struct, frozen=True):
    """Display-safe projection of the U2 typed failure audit."""

    failure_category: str | None = None
    error: str | None = None
    installed_path: str | None = None
    candidate_reference: str | None = None


def classify_import_job_display(
    job: ImportJob,
    *,
    queue_position: int,
) -> ImportJobDisplay:
    """Classify importer work without duplicating lifecycle copy in JS."""
    if job.status not in ("queued", "running", "recovery_required"):
        raise ValueError(
            f"timeline display requires an active import job, got {job.status!r}"
        )
    if queue_position < 0:
        raise ValueError("queue_position must be non-negative")
    if job.status == "recovery_required":
        return ImportJobDisplay(
            badge="Recovery required",
            badge_class="badge-failed",
            border_color="#a33",
            summary=(job.message or job.error or "Automatic replay refused"),
        )
    if job.status == "running":
        return ImportJobDisplay(
            badge="Importing",
            badge_class="badge-force",
            border_color="#36c",
            summary=(
                job.message or job.error or job.preview_message
                or job.preview_error or ""
            ),
        )

    preview = job.preview_status
    if preview == "evidence_ready":
        badge = "Next check" if queue_position == 0 else "Ready check"
        badge_class, border_color = "badge-new", "#1a4a2a"
    elif preview == "would_import":
        badge = "Next legacy check" if queue_position == 0 else "Legacy ready"
        badge_class, border_color = "badge-new", "#1a4a2a"
    elif preview == "running":
        badge, badge_class, border_color = "Previewing", "badge-warn", "#a93"
    elif preview == "waiting":
        badge, badge_class, border_color = (
            "Waiting preview", "badge-library", "#1a3a5a")
    elif preview == "confident_reject":
        badge, badge_class, border_color = (
            "Preview reject", "badge-failed", "#a33")
    elif preview == "measurement_failed":
        badge, badge_class, border_color = (
            "Measurement failed", "badge-failed", "#a33")
    elif preview == "uncertain":
        badge, badge_class, border_color = "Uncertain", "badge-warn", "#a93"
    elif preview == "error":
        badge, badge_class, border_color = (
            "Preview error", "badge-failed", "#a33")
    else:
        badge, badge_class, border_color = (
            "Queued", "badge-library", "#1a3a5a")

    return ImportJobDisplay(
        badge=badge,
        badge_class=badge_class,
        border_color=border_color,
        summary=(
            job.preview_message or job.message or job.preview_error
            or job.error or ""
        ),
    )


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
    triage = _extract_wrong_match_triage(entry)
    have_analysis = _extract_have_analysis_diagnostics(entry)
    failure = _present_entry_failure(entry)
    core = _classify(
        entry,
        triage_action=triage["action"],
        have_analysis=have_analysis,
        failure=failure,
    )
    summary = _build_summary(
        entry, core.badge, core.verdict, triage["summary"],
        peer_attributable=failure.peer_attributable,
    )
    downloaded_label = _build_downloaded_label(entry)
    (
        existing_format,
        existing_min_bitrate,
        existing_avg_bitrate,
        existing_median_bitrate,
    ) = _extract_existing_measurement(entry)
    materialized = _extract_materialized_measurement(entry)
    lineage = _extract_quality_lineage(entry)
    disambig_reason, disambig_detail = _extract_disambiguation_failure(entry)
    bad_extensions = _extract_bad_extensions(entry)
    basis = _entry_comparison_basis(entry)
    spectral = _extract_attempt_spectral(entry)
    candidate_measurement = triage["candidate_measurement"]
    current_measurement = triage["current_measurement"]
    if candidate_measurement is not None:
        lineage = (
            candidate_measurement.format,
            candidate_measurement.min_bitrate_kbps,
            candidate_measurement.avg_bitrate_kbps,
            candidate_measurement.median_bitrate_kbps,
            None,
            None,
        )
        spectral = (
            candidate_measurement.spectral_grade,
            candidate_measurement.spectral_bitrate_kbps,
            spectral[2], spectral[3], spectral[4], spectral[5],
            spectral[6], spectral[7],
        )
    if current_measurement is not None:
        existing_format = current_measurement.format
        existing_min_bitrate = current_measurement.min_bitrate_kbps
        existing_avg_bitrate = current_measurement.avg_bitrate_kbps
        existing_median_bitrate = current_measurement.median_bitrate_kbps
        spectral = (
            spectral[0], spectral[1],
            current_measurement.spectral_grade,
            current_measurement.spectral_bitrate_kbps,
            spectral[4], spectral[5], spectral[6], spectral[7],
        )
    if triage["comparison_basis"] is not None:
        basis = triage["comparison_basis"]
    candidate_v0 = triage["candidate_v0_probe"]
    current_v0 = triage["current_v0_probe"]
    candidate_audio_is_corrupt = _candidate_audio_is_corrupt(
        entry,
        triage["preview_decision"],
    )
    if candidate_audio_is_corrupt:
        # A decoder failure is terminal evidence about the source bytes. A
        # partial spectral diagnostic can still exist, but presenting it next
        # to a zero bitrate makes it look like a quality assessment. Retain
        # raw ImportResult/triage facts; only the shared display projection
        # declines to compare corrupt input as audio quality.
        downloaded_label = _codec_only_downloaded_label(entry, lineage[0])
        lineage = (lineage[0], None, None, None, None, None)
        materialized = (None, None, None, None)
        spectral = (
            None, None, spectral[2], spectral[3], None, None,
            spectral[6], spectral[7],
        )
        candidate_v0 = None
        basis = None
    return ClassifiedEntry(
        badge=core.badge, badge_class=core.badge_class,
        border_color=core.border_color, verdict=core.verdict,
        summary=summary, downloaded_label=downloaded_label,
        actual_min_bitrate=(
            None if candidate_audio_is_corrupt
            else entry.actual_min_bitrate
        ),
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
        source_format=lineage[0],
        source_min_bitrate=lineage[1],
        source_avg_bitrate=lineage[2],
        source_median_bitrate=lineage[3],
        target_contract_format=lineage[4],
        legacy_projection_version=lineage[5],
        materialized_format=materialized[0],
        materialized_min_bitrate=materialized[1],
        materialized_avg_bitrate=materialized[2],
        materialized_median_bitrate=materialized[3],
        spectral_grade=spectral[0],
        spectral_bitrate=spectral[1],
        existing_spectral_grade=spectral[2],
        existing_spectral_bitrate=spectral[3],
        spectral_attempted=spectral[4],
        spectral_error=spectral[5],
        existing_spectral_attempted=spectral[6],
        existing_spectral_error=spectral[7],
        existing_min_bitrate=existing_min_bitrate,
        existing_avg_bitrate=existing_avg_bitrate,
        existing_median_bitrate=existing_median_bitrate,
        v0_probe_kind=(
            None if candidate_audio_is_corrupt else (
                candidate_v0.kind
                if candidate_v0 is not None else entry.v0_probe_kind
            )
        ),
        v0_probe_min_bitrate=(
            None if candidate_audio_is_corrupt else (
                candidate_v0.min_bitrate_kbps
                if candidate_v0 is not None else entry.v0_probe_min_bitrate
            )
        ),
        v0_probe_avg_bitrate=(
            None if candidate_audio_is_corrupt else (
                candidate_v0.avg_bitrate_kbps
                if candidate_v0 is not None else entry.v0_probe_avg_bitrate
            )
        ),
        v0_probe_median_bitrate=(
            None if candidate_audio_is_corrupt else (
                candidate_v0.median_bitrate_kbps
                if candidate_v0 is not None else entry.v0_probe_median_bitrate
            )
        ),
        existing_v0_probe_kind=(
            current_v0.kind
            if current_v0 is not None else entry.existing_v0_probe_kind
        ),
        existing_v0_probe_min_bitrate=(
            current_v0.min_bitrate_kbps
            if current_v0 is not None else entry.existing_v0_probe_min_bitrate
        ),
        existing_v0_probe_avg_bitrate=(
            current_v0.avg_bitrate_kbps
            if current_v0 is not None else entry.existing_v0_probe_avg_bitrate
        ),
        existing_v0_probe_median_bitrate=(
            current_v0.median_bitrate_kbps
            if current_v0 is not None else entry.existing_v0_probe_median_bitrate
        ),
        transfer_message=failure.transfer_message,
        transfer_message_label=failure.transfer_message_label,
        beets_detail_label=failure.beets_detail_label,
        failure_category=have_analysis.failure_category,
        analysis_error=have_analysis.error,
        installed_path=have_analysis.installed_path,
        candidate_reference=have_analysis.candidate_reference,
    )


def _present_entry_failure(entry: LogEntry) -> FailurePresentation:
    """Derive this row's failure copy from its persisted evidence.

    The ONE place Recents turns a failure row's raw persisted text into
    operator copy (issue #868). Everything it needs is already on the row;
    it writes nothing back.
    """
    return present_failure(FailureEvidence(
        outcome=entry.outcome,
        error_message=entry.error_message,
        beets_detail=entry.beets_detail,
        beets_scenario=entry.beets_scenario,
        soulseek_username=entry.soulseek_username,
        transfer_detail=decode_transfer_detail(entry.transfer_detail),
        transfer_detail_unreadable=transfer_detail_unreadable(
            entry.transfer_detail),
    ))


def _extract_have_analysis_diagnostics(
    entry: LogEntry,
) -> _HaveAnalysisDiagnostics:
    """Decode a HAVE-analysis audit without letting legacy damage 500 Recents."""

    if entry.outcome != "have_analysis_error":
        return _HaveAnalysisDiagnostics()

    failure: HaveAnalysisFailure | None = None
    try:
        if isinstance(entry.validation_result, str):
            failure = msgspec.json.decode(
                entry.validation_result,
                type=HaveAnalysisFailure,
            )
        elif entry.validation_result is not None:
            failure = msgspec.convert(
                entry.validation_result,
                type=HaveAnalysisFailure,
                strict=True,
            )
    except (msgspec.ValidationError, msgspec.DecodeError):
        # The top-level columns still carry enough information for a useful
        # environment-failure card if one historical JSONB value is damaged.
        failure = None

    return _HaveAnalysisDiagnostics(
        failure_category=(
            failure.failure_category if failure is not None else None
        ),
        error=(
            failure.error if failure is not None else entry.error_message
        ),
        installed_path=(
            failure.installed_path
            if failure is not None and failure.installed_path is not None
            else entry.download_path
        ),
        candidate_reference=(
            failure.candidate_reference
            if failure is not None and failure.candidate_reference is not None
            else entry.staged_path
        ),
    )


def _extract_quality_lineage(
    entry: LogEntry,
) -> tuple[
    str | None,
    int | None,
    int | None,
    int | None,
    str | None,
    int | None,
]:
    """Expose v3 source/contract facts without projecting historical proxies."""

    ir = _parse_import_result(entry)
    if ir is None:
        return (
            entry.source_format,
            entry.source_min_bitrate,
            entry.source_avg_bitrate,
            entry.source_median_bitrate,
            None,
            None,
        )
    target = (
        ir.target_quality_contract.format
        if ir.target_quality_contract is not None
        else None
    )
    if ir.legacy_projection_version is not None:
        return None, None, None, None, target, ir.legacy_projection_version
    source = ir.source_measurement
    return (
        source.format if source is not None else entry.source_format,
        (
            source.min_bitrate_kbps
            if source is not None else entry.source_min_bitrate
        ),
        (
            source.avg_bitrate_kbps
            if source is not None else entry.source_avg_bitrate
        ),
        (
            source.median_bitrate_kbps
            if source is not None else entry.source_median_bitrate
        ),
        target,
        None,
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


def _extract_existing_measurement(
    entry: LogEntry,
) -> tuple[str | None, int | None, int | None, int | None]:
    """The complete on-disk bitrate snapshot used by this attempt."""
    ir = _parse_import_result(entry)
    if ir is None or ir.current_measurement is None:
        return (None, entry.existing_min_bitrate, None, None)
    measurement = ir.current_measurement
    return (
        measurement.format,
        measurement.min_bitrate_kbps,
        measurement.avg_bitrate_kbps,
        measurement.median_bitrate_kbps,
    )


def _extract_materialized_measurement(
    entry: LogEntry,
) -> tuple[str | None, int | None, int | None, int | None]:
    """Project the measured output without relabelling decision evidence."""
    ir = _parse_import_result(entry)
    if ir is None or ir.materialized_measurement is None:
        return (None, None, None, None)
    measurement = ir.materialized_measurement
    return (
        measurement.format,
        measurement.min_bitrate_kbps,
        measurement.avg_bitrate_kbps,
        measurement.median_bitrate_kbps,
    )


def _extract_disambiguation_failure(
    entry: LogEntry,
) -> tuple[str | None, str | None]:
    """Pull ``postflight.disambiguation_failure.{reason, detail}`` out of the
    ImportResult JSONB. Returns ``(None, None)`` when no failure is present
    or the blob is missing/unreadable — callers render the chip conditionally.
    """
    ir = _parse_import_result(entry)
    if ir is None:
        return (None, None)
    fail = ir.postflight.disambiguation_failure
    if fail is None:
        return (None, None)
    return (fail.reason, fail.detail)


def _extract_bad_extensions(entry: LogEntry) -> list[str]:
    """Pull postflight bad-extension filenames out of ImportResult JSONB."""
    ir = _parse_import_result(entry)
    if ir is None:
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
        "candidate_measurement": None,
        "current_measurement": None,
        "candidate_v0_probe": None,
        "current_v0_probe": None,
        "comparison_basis": None,
    }


def _humanize_token(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("_", " ").replace("-", " ").strip()


def _wrong_match_action_label(action: str | None) -> str | None:
    """Operator label for a persisted wrong-match triage action.

    Every literal matched here is one ``lib/wrong_match_cleanup_service.py``
    or ``lib/wrong_match_delete_service.py`` actually writes, except
    ``preview_backfilled`` which is historical (59 live rows, 2026-04-26 to
    2026-04-28). Anything else falls through to the humanized token, so an
    unhandled producer action still reads as words rather than inventing a
    fact. Audited by ``tests/test_classify_producer_audit.py``.
    """
    if action == "deleted_reject":
        return "download deleted"
    if action == "deleted_verified_lossless_parent":
        return "download deleted: verified-lossless parent"
    if action == "delete_failed":
        return "delete failed"
    if action == "kept_would_import":
        return "download kept: would import"
    if action == "kept_uncertain":
        return "download kept: uncertain"
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

    label = _wrong_match_action_label(action)

    if action == "deleted_reject":
        detail = (_humanize_token(reason)
                  or _humanize_token(preview_decision)
                  or _humanize_token(preview_verdict))
        return (
            f"download deleted: {detail}" if detail else "download deleted"
        )

    if action == "deleted_verified_lossless_parent":
        return "download deleted: verified-lossless parent in library"

    if action == "kept_would_import":
        return "download kept: would import"

    if action == "kept_uncertain":
        detail = (_humanize_token(reason)
                  or _humanize_token(preview_decision)
                  or _humanize_token(preview_verdict))
        return (
            f"download kept: {detail}" if detail else "download kept: uncertain"
        )

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
        "candidate_measurement": triage.candidate_measurement,
        "current_measurement": triage.current_measurement,
        "candidate_v0_probe": triage.candidate_v0_probe,
        "current_v0_probe": triage.current_v0_probe,
        "comparison_basis": triage.comparison_basis,
    }


def _beets_returned_no_candidates(entry: LogEntry) -> bool:
    """Whether a real beets run POSITIVELY recorded an empty candidate set.

    ``lib/beets.py`` writes ``mbid_not_found`` whenever the requested release
    ID is absent from ``cm.candidates``, empty set or not, so only the blob it
    wrote alongside separates "beets returned nothing" from "beets returned
    siblings". The discriminator has to prove beets RAN, not merely that the
    blob is ValidationResult-shaped: ``valid`` defaults to ``False`` and is
    always emitted, so most rejection blobs on disk are synthesized stubs
    (``ValidationResult(distance=…, scenario=…, detail=…)`` in
    ``lib/download_rejection.py`` / ``lib/dispatch/outcome_actions.py``) that
    carry an empty ``candidates`` list purely because beets was never
    consulted (issue #882 review F4).

    ``items`` and ``recommendation`` are written together, and only, by the
    ``choose_match`` handler in ``lib/beets.py`` — the local tracks the
    harness reported and beets' own confidence in the match it proposed — so
    both non-empty is positive evidence of a real run. Live on 2026-07-26 the
    two populations separate cleanly: of every row carrying a zero-candidate
    validation blob, the 18 with both signals all have
    ``recommendation='none'`` and 2-20 items, and the other 911 have neither.
    Anything short of both fails closed to the general sentence, which is
    true of an empty candidate set anyway.
    """
    try:
        envelope = decode_validation_envelope(entry.validation_result)
    except (msgspec.ValidationError, json.JSONDecodeError):
        return False
    beets_ran = bool(envelope.items) and bool(envelope.recommendation)
    return beets_ran and not envelope.candidates


def _target_extra_track_count(entry: LogEntry) -> int | None:
    """How many of the matched release's tracks had no local file.

    Read from the producer's own structured evidence, never from its prose:
    ``lib/beets.py``'s ``choose_match`` handler flags the requested
    release's candidate with ``is_target`` and persists beets'
    ``extra_tracks`` — the release tracks its item assignment left
    unmatched — inside ``validation_result.candidates``. That array is
    exactly what the producer counts to compose its own ``detail`` string,
    so this reads the count rather than parsing the sentence. Live on
    2026-07-26 all 45 ``extra_tracks`` rows carry the array and its length
    equals the number in their persisted detail.

    ``None`` when no target candidate carries a non-empty array: the
    producer only writes this scenario when the array is non-empty, so an
    empty or absent one is missing evidence, not a count of zero.
    """
    try:
        envelope = decode_validation_envelope(entry.validation_result)
    except (msgspec.ValidationError, json.JSONDecodeError):
        return None
    for candidate in envelope.candidates:
        if candidate.get("is_target") is not True:
            continue
        extra = candidate.get("extra_tracks")
        if not is_list_like(extra):
            continue
        count = len(json_list(extra))
        if count > 0:
            return count
    return None


def _recorded_import_error(entry: LogEntry) -> str | None:
    """The importer's own recorded reason for this attempt, bounded.

    ``harness/import_one.py`` sets ``ImportResult.error`` at every decision
    site that ends an import early, and the rejection writer denormalizes
    it into ``error_message``; both are read here so a row missing the
    typed blob still shows what its producer said. The text is passed
    through, never interpreted — it is the only thing that separates the
    several producer sites sharing one decision name.
    """
    ir = _parse_import_result(entry)
    recorded = (ir.error if ir is not None else None) or entry.error_message
    if not recorded:
        return None
    return bounded_text(recorded, limit=MAX_DIAGNOSTIC_CHARS) or None


def _candidate_audio_is_corrupt(
    entry: LogEntry,
    triage_preview_decision: str | None,
) -> bool:
    """Whether the candidate is corrupt on either persisted audit path."""
    return (
        _entry_rejection_decision(entry) == "audio_corrupt"
        or triage_preview_decision == "audio_corrupt"
    )


def _classify(
    entry: LogEntry,
    *,
    failure: FailurePresentation,
    triage_action: str | None = None,
    have_analysis: _HaveAnalysisDiagnostics | None = None,
) -> _Classification:
    """Build the typed core classification for one log entry.

    ``failure`` is the row's already-derived failure copy — required, so the
    presenter runs exactly once per entry and the three failure branches
    cannot quietly grow a second wording.
    """

    # --- Installed-HAVE analysis failure (environment, not quality) ---
    if entry.outcome == "have_analysis_error":
        diagnostics = have_analysis or _HaveAnalysisDiagnostics()
        category = (
            _humanize_token(diagnostics.failure_category)
            if diagnostics.failure_category
            else "unknown analyser failure"
        )
        if entry.request_status == "wanted":
            lifecycle = "Search remains open; a future download will retry."
        elif entry.request_status == "unsearchable":
            lifecycle = "Operator search stop remains in place."
        else:
            lifecycle = "The request lifecycle was preserved."
        verdict = f"Installed HAVE analysis failed ({category}). {lifecycle}"
        return _Classification(
            "Environment failure", "badge-warn", "#a86f20", verdict,
        )

    # --- Verified-lossless proof lock (non-punitive decline) ---
    # The archival copy is proof-verified and the request stays imported;
    # the candidate is declined without denylist or narrowing. Render as a
    # lock, not a rejection, so the audit trail is not mislabeled.
    if (entry.outcome == "rejected"
            and _entry_rejection_decision(entry) == "verified_lossless_locked"):
        return _Classification(
            "Proof locked", "badge-library", "#1a3a5a",
            _verified_lossless_locked_verdict(),
        )

    # --- Rejected ---
    if entry.outcome == "rejected":
        verdict = _rejection_verdict(entry)
        if triage_action in ("kept_would_import", "kept_uncertain"):
            return _Classification(
                "Triaged · download kept", "badge-warn", "#a33", verdict
            )
        if triage_action in ("deleted_reject", "deleted_verified_lossless_parent"):
            return _Classification(
                "Triaged · download deleted", "badge-rejected", "#a33", verdict
            )
        return _Classification("Rejected", "badge-rejected", "#a33", verdict)

    # --- Preview measurement failure ---
    # The worker persists the exact operator-facing diagnostic in
    # ``error_message``. Historical rows predate that write contract but carry
    # the same detail in ``beets_detail``, so keep those cards readable too.
    # ``lib.failure_presentation`` owns the wording (issue #868).
    if entry.outcome == "measurement_failed":
        return _Classification(
            "Measurement failed", "badge-failed", "#a33",
            failure.verdict or "Measurement failed",
        )

    # --- Timeout (download-phase; outcome="timeout" is written ONLY by
    # lib/download.py::_timeout_album — error_message is the real
    # per-file evidence summary, issue #564 C5, when any was captured, and
    # transfer_detail is the per-file audit behind it) ---
    if entry.outcome == "timeout":
        return _Classification(
            "Failed", "badge-failed", "#a33",
            failure.verdict or "Download failed",
        )

    # --- Failed (import-phase, plus the materialize/staging failures that
    # never reached an import — the presenter labels those honestly instead
    # of prefixing "Import error:" onto everything) ---
    if entry.outcome == "failed":
        verdict = (
            failure.verdict
            or _quality_verdict_from_import_result(entry)
            or "Import error"
        )
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
            if basis is not None and basis.verified_lossless_bypass:
                verdict = _proof_upgrade_verdict_from_basis(
                    basis,
                    entry.was_converted,
                    entry.original_filetype,
                )
            elif basis is not None:
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
        return ImportResult.from_json(raw)
    except (json.JSONDecodeError, TypeError, KeyError, ValueError,
            msgspec.ValidationError):
        return None


def _entry_decision(entry: LogEntry) -> str | None:
    ir = _parse_import_result(entry)
    if ir is not None and ir.decision:
        return ir.decision
    return entry.beets_scenario


def _entry_rejection_decision(entry: LogEntry) -> str | None:
    """Prefer an ImportResult only when it actually records a rejection."""
    ir = _parse_import_result(entry)
    if (
        ir is not None
        and ir.decision
        and dispatch_action(ir.decision).record_rejection
    ):
        return ir.decision
    return entry.beets_scenario


def _entry_comparison_basis(entry: LogEntry) -> QualityComparisonBasis | None:
    ir = _parse_import_result(entry)
    if ir is None:
        return None
    return ir.comparison_basis


def _basis_value_phrase(metric: str, value: int | None, clamped: bool) -> str:
    if metric == "contract":
        return "contract"
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
    # spectral_tiebreak (issue #813 Finding 1) carries the same clamped
    # min(metric, spectral floor) value as the rank branch — the metric
    # label would lie there too.
    clamped = basis.spectral_clamped and basis.branch in ("rank", "spectral_tiebreak")
    new_val = _basis_value_phrase(basis.new_metric, basis.new_value_kbps, clamped)
    ex_val = _basis_value_phrase(
        basis.existing_metric, basis.existing_value_kbps, clamped)

    if basis.verdict == "better":
        if basis.branch in ("metric_tiebreak", "spectral_tiebreak"):
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
    original_ft: str | None,
    is_verified_lossless: bool,
) -> str:
    """Basis-driven twin of _upgrade_verdict, keeping the legacy suffixes."""
    parts = [_verdict_from_basis(basis)]
    if was_converted and original_ft:
        parts.append(f"from {original_ft.upper()}")
    if is_verified_lossless and not basis.verified_lossless_bypass:
        parts.append("verified lossless")
    return ", ".join(parts)


def _proof_upgrade_verdict_from_basis(
    basis: QualityComparisonBasis,
    was_converted: bool,
    original_ft: str | None,
) -> str:
    """Describe an equivalent-quality replacement made only to gain proof."""
    new_format = (basis.new_format or "?").upper()
    existing_format = (basis.existing_format or "?").upper()
    parts = [
        (
            f"Proof upgrade: {basis.verdict} quality — "
            f"{new_format} vs {existing_format}"
        ),
    ]
    if was_converted and original_ft:
        parts.append(f"from {original_ft.upper()}")
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

    Priority chain for legacy decision-time displays:
        1. ``entry.actual_min_bitrate`` — denormalized column, populated by
           ``_populate_dl_info_from_import_result`` on the auto-import path
           since the ``actual_min_bitrate`` fix.
        2. ``ir.source_measurement.min_bitrate_kbps`` — authoritative JSONB,
           present on every row. Fixes historical rows (pre-column-fix)
           retroactively without a backfill migration.
        3. ``entry.bitrate`` (bps) — legacy container bitrate, last resort.

    New successful imports expose ``materialized_measurement`` separately;
    callers describing output bytes must use that instead. ``spectral_bitrate``
    is a cliff estimate ("what was the original source?"),
    not the file's actual bitrate. It must never appear here.
    """
    if entry.actual_min_bitrate:
        return entry.actual_min_bitrate
    ir = _parse_import_result(entry)
    if (
        ir is not None
        and ir.source_measurement is not None
        and ir.source_measurement.min_bitrate_kbps is not None
    ):
        return ir.source_measurement.min_bitrate_kbps
    if entry.source_min_bitrate:
        return entry.source_min_bitrate
    if entry.bitrate:
        return entry.bitrate // 1000
    return None


def _quality_verdict_from_import_result(entry: LogEntry) -> str | None:
    """Derive a quality comparison verdict from ImportResult JSONB.

    Used by both rejected and failed outcomes — single source of truth
    for "X is not better than Y" messages.
    """
    ir = _parse_import_result(entry)
    if ir is None:
        return None

    # Rejected rows already carry the complete measured comparison in the
    # fixed IN/HAVE/Spectral/V0 evidence schema. Keep verdict prose to one
    # short decision-class grammar so the same rejection never reads like
    # four different policies depending on which evidence path produced it.
    if ir.decision == "downgrade":
        return "Quality not better than on-disk copy; searching continues"
    if ir.decision == "transcode_downgrade":
        return "Transcode not better than on-disk copy; searching continues"
    if ir.decision in (
        "suspect_lossless_downgrade",
        "suspect_lossless_probe_missing",
    ):
        return _provisional_verdict(entry, imported=False)
    if ir.decision == "lossless_source_locked":
        return _lossless_source_locked_verdict()

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
        if not final_format and ir.target_quality_contract is not None:
            final_format = ir.target_quality_contract.format
        if (
            not final_format
            and ir.legacy_projection_version is not None
            and ir.source_measurement is not None
        ):
            final_format = ir.source_measurement.format
    return candidate_avg, existing_avg, final_format


def _spectral_phrase(entry: LogEntry) -> str | None:
    if not entry.spectral_grade:
        return None
    phrase = f"spectral {entry.spectral_grade}"
    if entry.spectral_bitrate:
        phrase += f" ~{entry.spectral_bitrate}kbps"
    return phrase


def _lossless_source_locked_verdict() -> str:
    """Verdict copy for the lossless-source lock rejection.

    Fires when a lossy candidate is offered against an existing album whose
    original lossless-source V0 probe is already recorded. Measurements stay
    in the fixed evidence schema; this sentence names only the policy reason.
    """
    return (
        "Lossless-source locked; only another lossless source can override; "
        "searching continues"
    )


def _verified_lossless_locked_verdict() -> str:
    """Verdict copy for the verified-lossless proof lock.

    Fires when an automatic candidate completes against an album whose
    verified-lossless proof is already recorded: acquisition is complete,
    the candidate is declined without blame (no denylist, no narrowing),
    and only operator actions (Replace, re-request, force-import) reopen it.
    """
    return (
        "Verified lossless already on disk; automatic candidate declined "
        "(no denylist); acquisition is complete"
    )


def _provisional_verdict(entry: LogEntry, *, imported: bool) -> str:
    if not imported:
        return (
            "Suspect lossless source not better than on-disk copy; "
            "searching continues"
        )
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
    if final_format:
        parts.append(f"stored as {final_format}")
    parts.append("source denylisted")
    parts.append("searching continues")
    return "Provisional lossless source: " + "; ".join(parts)


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
    # Validation and import decisions describe different stages. A candidate
    # can be a strong pressing match and still be rejected by the importer
    # (for example, a folder containing both FLAC and OGG tracks). The final
    # ImportResult decision owns the rejection headline; beets_scenario is the
    # fallback for validation-only rejects and remains available as forensic
    # evidence in the row payload.
    scenario = _entry_rejection_decision(entry)

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
            return _lossless_source_locked_verdict()
        if scenario == "transcode_downgrade":
            return "Transcode not better than on-disk copy; searching continues"
        return "Quality not better than on-disk copy; searching continues"

    if scenario == "spectral_reject":
        return (
            "Spectral quality not better than on-disk copy; "
            "searching continues"
        )

    if scenario == "high_distance":
        dist = (f"{float(entry.beets_distance):.3f}"
                if entry.beets_distance is not None else "?")
        return f"Wrong match (dist {dist})"

    if scenario == "audio_corrupt":
        return "Corrupt audio files detected"

    if scenario == "mixed_source":
        return "Mixed lossless+lossy source"

    # ``lib/beets.py`` writes this from inside the branch that FOUND the
    # requested release among beets' candidates: ``cand.mbid ==
    # mb_release_id`` set ``mbid_found``, recorded the distance, and then
    # saw ``len(cand.extra_tracks) > 0`` — release tracks that beets' own
    # item-to-track assignment left unmatched.
    #
    # Two things the producer knows and the sentence therefore says: the
    # release is the right one, and part of it has no local audio. Two it
    # does NOT know, and the sentence therefore avoids: WHY (a partial
    # download, a peer's own incomplete rip and a different track layout
    # are all consistent with an unmatched track), and whether the pressing
    # was otherwise a good match — this branch fires BEFORE the distance
    # threshold is consulted, so the candidate may well have been inside it.
    if scenario == "extra_tracks":
        unmatched = _target_extra_track_count(entry)
        if unmatched is None:
            return "Requested release has tracks with no matching local file"
        return (
            f"Requested release has {unmatched} "
            f"track{'' if unmatched == 1 else 's'} with no matching local file"
        )

    # Written by the two manifest guards that reject before beets is ever
    # consulted: ``lib/download_validation.py`` on the staged auto-import
    # folder and ``lib/dispatch/manifest_guard.py`` on a force/manual
    # import source. Both compare the folder against the audio manifest
    # this attempt selected and both record the difference in
    # ``beets_detail``, which the card shows underneath.
    #
    # The sentence says MISMATCH and stops there. ``check_audio_manifest``
    # reports extra AND missing audio and ``_check_staged_audio_manifest``
    # labels either one ``untracked_audio``, so "contains extra audio" —
    # true of every live row today — is a claim the producer does not make
    # and would be false of a source that is merely short.
    if scenario == "untracked_audio":
        return "Import folder does not match the selected audio manifest"

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

    # ``lib/beets.py`` sets this from ``if not result.mbid_found``: the
    # requested release ID is not in the candidate set beets emitted. That
    # fires whether or not the set was empty — 18 of the 50 live rows carry
    # an empty one — and the two cases send the operator to different
    # places, so each gets the sentence its own evidence supports.
    #
    # The empty arm names the release-ID LOOKUP, not the folder, because
    # the folder cannot be the discriminator: ``lib/beets.py`` always
    # passes ``--search-id``, the harness maps it to
    # ``config["import"]["search_ids"]``, and beets' ``tag_album`` then
    # takes its ``if search_ids:`` branch — deriving candidates from
    # ``albums_for_ids`` alone and skipping the metadata/text search
    # entirely. Live confirmation: every one of the 13 requests behind
    # those 18 rows has sibling attempts on the SAME release ID that did
    # return candidates (1 to 115 each). It says "Beets" rather than
    # "MusicBrainz" because the ID is not always an MBID — two of the 18
    # requested Discogs release IDs, which ``albums_for_ids`` dispatches
    # across metadata plugins.
    #
    # The copy this replaced was keyed on ``no_candidates``, a string no
    # producer has ever emitted (issue #882): the 50 live rows carrying the
    # real literal fell through to the raw-token fallback while the fluent
    # sentence sat behind a key nothing could reach.
    if scenario == "mbid_not_found":
        if _beets_returned_no_candidates(entry):
            return (
                "Beets returned no match candidates for the requested "
                "release ID"
            )
        return "Requested release ID not among the match candidates"

    # ``lib/beets.py`` writes this when its stdout loop finished, WITHOUT
    # recording an error, and never decoded a ``choose_match``: beets
    # offered nothing for the folder, so nothing was weighed against the
    # requested release ID.
    #
    # The sentence names that OBSERVATION and stops there, because the
    # observation is all the producer knows. No importable audio, a session
    # cut short, and a harness that exited quietly are all consistent with
    # it — the discriminating facts (which harness messages arrived, whether
    # a session_end was announced, the stderr tail) are persisted as
    # ``validation_result.harness_session`` and summarised into
    # ``beets_detail``, which the card shows beneath this line. Claiming a
    # cause here would be exactly the #868/#882 defect in a new place.
    #
    # The error branches are deliberately a DIFFERENT name below, because
    # one of them — the strict-decode refusal — is a case where beets DID
    # offer a match and Cratedigger declined to decode it. Merging them
    # would put a falsehood on the card at exactly the moment a beets
    # version bump changes a field type, which is the 2026-06-28/29 shape
    # this branch exists to explain (issue #888 review F1).
    #
    # 276 live rows across 215 requests carried this rejection with a NULL
    # scenario before issue #888 named it, so the card read "Rejected" and
    # nothing else; 254 of those fell on 2026-06-28/29, the beets 2.12
    # migration window.
    if scenario == "no_choose_match":
        return "Beets ended without offering a match to review"

    # ``lib/beets.py`` writes this when validation recorded an error before
    # any match could be reviewed: the harness would not start, the strict
    # wire decode refused a ``choose_match``, the read loop raised, or the
    # 120s timeout fired. The sentence claims only that — never that beets
    # offered nothing — and points at the error the producer recorded rather
    # than interpreting it.
    if scenario == "validation_error":
        recorded = (entry.error_message or "").strip()
        if recorded:
            return (
                "Validation failed before a match could be reviewed: "
                f"{recorded}"
            )
        return "Validation failed before a match could be reviewed"

    # --- The importer subprocess's own decision names -------------------
    #
    # ``harness/import_one.py`` owns the group below. They share one shape:
    # the decision names the STAGE that ended the attempt, and
    # ``ImportResult.error`` names the reason. What differs is whether the
    # decision name already FIXES that reason, and that is what decides
    # whether the sentence quotes.
    #
    # Where one name covers several producer sites and arbitrary beets/OS
    # exceptions, the recorded text is the only discriminator and is passed
    # through bounded. Where the name is written from exactly one condition,
    # the sentence states that condition instead — the recorded string there
    # adds nothing the name has not already said, and the card's Detail row
    # still shows it verbatim.

    # Set wherever beets RAN and the library did not end up as expected: a
    # nonzero harness rc that is not the duplicate-guard or MBID-missing
    # one, plus the four post-import / post-flight consistency checks
    # (release absent, two rows for one release, post-flight lookup empty,
    # post-flight resolving to a different album id). All 47 live rows
    # carry a recorded reason.
    if scenario == "import_failed":
        recorded = _recorded_import_error(entry)
        if recorded:
            return f"Import failed: {recorded}"
        # True of every site: the rc branches never created a row, and the
        # consistency branches found the wrong one. "Did not complete"
        # would be false of the latter, which completed and completed wrong.
        return "Import did not leave beets in the expected state"

    # ``import_one.py``'s top-level ``except Exception`` envelope: the
    # helper died before any normal exit, recording only
    # ``f"{type(exc).__name__}: {exc}"``. Live examples are a
    # UnicodeDecodeError on beets output and a missing ``beet`` binary —
    # environment faults, which is exactly why the raw text leads.
    if scenario == "crash":
        recorded = _recorded_import_error(entry)
        if recorded:
            return f"Import crashed: {recorded}"
        return "Import crashed with an unhandled exception"

    # ``run_import`` returns rc 4 from ONE place: the ``choose_match``
    # handler found no candidate whose album id is the requested release,
    # answered ``skip``, and returned. Other candidates may well have been
    # offered — the requested one was not among them — and nothing was
    # applied, so beets was left untouched.
    #
    # This is the import-time twin of the validation-time
    # ``mbid_not_found`` above; naming the candidate SET each one looked in
    # is what keeps two identically-worded rejections apart.
    if scenario == "mbid_missing":
        return (
            "Requested release ID was not among the import candidates; "
            "nothing was applied"
        )

    # Both producer sites sit inside the block that applies the preview's
    # persisted quality-evidence action, and both ``_emit_and_exit`` before
    # ``run_import`` is reached: one when the recorded decision does not
    # authorize an import at all, one for any exception raised loading,
    # snapshot-checking or materializing the action (the live pair are
    # conversion failures). "Before beets ran" is structural, not inferred.
    if scenario == "quality_evidence_action_failed":
        recorded = _recorded_import_error(entry)
        if recorded:
            return f"Quality-evidence action failed before beets ran: {recorded}"
        return "Quality-evidence action failed before beets ran"

    # ``lib/dispatch/core.py``'s auto-import ``except Exception``. It calls
    # ``logger.exception`` — so the traceback exists, in the journal — and
    # then persists the bare words "exception" and "unhandled exception in
    # auto-import" and nothing else. Quoting either would hand the operator
    # their own token back, so the sentence says what happened and where
    # the detail actually is.
    if scenario == "exception":
        return (
            "Auto-import raised an unhandled exception; the traceback is in "
            "the service log"
        )

    # Historical: emitted by a pre-2026-03-24 revision, one live row, no
    # current producer — which is why the audit registers it as historical.
    if scenario == "album_name_mismatch":
        return "Album name mismatch"

    if scenario == "nested_layout":
        return "Nested folder layout (flatten first)"

    # An unhandled producer scenario reads as words, not as a machine token —
    # the same doctrine ``_wrong_match_action_label`` has always applied, and
    # the one this module had two answers to before #882. Humanizing invents
    # no fact: it is the token itself, spelled for a human.
    return _humanize_token(scenario) or "Rejected"


def _upgrade_verdict(prev_br: int | None, cur_br: int | None,
                     was_converted: bool, original_ft: str | None,
                     is_verified_lossless: bool,
                     actual_filetype: str | None = None) -> str:
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

def _build_summary(
    entry: LogEntry,
    badge: str,
    verdict: str,
    triage_summary: str | None,
    *,
    peer_attributable: bool = True,
) -> str:
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

    # Cleanup is a later audit fact. Fold it into the one server-composed
    # summary instead of having JavaScript add a competing status badge.
    if badge.startswith("Triaged · ") and triage_summary:
        parts.append(triage_summary)

    # The peer is the summary's trailing attribution \u2014 unless a humanized
    # failure verdict already named it AS a peer (issue #868), in which case
    # repeating it reads as "Peer X rejected all 29 files \u2026 \u00b7 X".
    #
    # The test is our own attribution grammar ("peer <name>"), not a bare
    # substring: a short or common username occurring inside a filename,
    # album title or a longer peer's name would otherwise silently lose its
    # attribution, and non-peer verdicts (curator_ban's "denylisted X")
    # keep the trailing attribution they have always had.
    # ``peer_attributable`` is False when the failure was OURS (slskd
    # writing to our own share). The list row is the one line the operator
    # reads, and appending the peer there put their name back on our fault
    # on 25 live rows (issue #868 review #12).
    if (
        entry.soulseek_username
        and peer_attributable
        and not any(
            f"peer {entry.soulseek_username}".casefold() in part.casefold()
            for part in parts
        )
    ):
        parts.append(entry.soulseek_username)

    return " \u00b7 ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Downloaded label — server-computed quality description of the download
# ---------------------------------------------------------------------------

def _build_downloaded_label(entry: LogEntry) -> str:
    """Build a label describing what was downloaded.

    Examples: "MP3 320", "FLAC (converted to MP3 V0)", "MP3 V2"
    """
    fmt = entry.actual_filetype or entry.source_format or entry.filetype or ""
    if not fmt:
        return ""

    # A comma-separated attempt format is a mixed-codec album, not one codec
    # with a meaningful shared bitrate tier. Preserve every measured format so
    # a mixed-source safety rejection can never render as an all-FLAC source.
    formats = list(dict.fromkeys(
        part.strip().upper() for part in fmt.split(",") if part.strip()
    ))
    if len(formats) > 1:
        return " + ".join(formats)

    br_kbps = _downloaded_min_bitrate_kbps(entry) or 0

    if entry.was_converted and entry.original_filetype:
        ir = _parse_import_result(entry)
        target = entry.final_format or (ir.final_format if ir is not None else None)
        target_label = target or legacy_floor_quality_label(fmt, br_kbps)
        return f"{entry.original_filetype.upper()} → {target_label.upper()}"

    return legacy_floor_quality_label(fmt, br_kbps) if br_kbps else fmt.upper()


def _codec_only_downloaded_label(entry: LogEntry, source_format: str | None) -> str:
    """Return only the corrupt candidate's measured codec identity.

    Corrupt bytes are not comparable quality evidence, so never carry their
    tier, target conversion, or raw fallback bitrate into the display label.
    """
    fmt = (
        source_format or entry.source_format or entry.slskd_filetype
        or entry.original_filetype or entry.filetype or entry.actual_filetype
    )
    if not fmt:
        return ""
    formats = list(dict.fromkeys(
        part.strip().upper() for part in fmt.split(",") if part.strip()
    ))
    return " + ".join(formats)
