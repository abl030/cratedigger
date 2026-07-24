"""Typed download-log presentation helpers shared by detail and pipeline views."""

from __future__ import annotations

import json

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


import msgspec

from lib.json_narrow import json_dict
from web.classify import ClassifiedEntry, LogEntry, classify_log_entry


@dataclass(frozen=True)
class ClassifiedDownloadLogRow:
    """One raw download_log row plus its shared UI classification."""

    entry: LogEntry
    classified: ClassifiedEntry


class DownloadHistoryViewRow(msgspec.Struct, frozen=True):
    """Frontend contract shared by detail-view download-history panels."""

    id: int
    request_id: int
    outcome: str
    badge: str
    badge_class: str
    border_color: str
    created_at: str | None
    beets_scenario: str | None
    beets_distance: float | None
    # Apply-time beets distance persisted by #863 in import_result JSONB
    # (None on rows predating it) — the card's Distance row shows it next
    # to the validate-time number (issue #865).
    apply_beets_distance: float | None
    source_download_log_id: int | None
    original_beets_distance: float | None
    beets_detail: str | None
    soulseek_username: str | None
    error_message: str | None
    download_path: str | None
    staged_path: str | None
    import_result: str | dict[str, object] | None
    validation_result: str | dict[str, object] | None
    filetype: str | None
    bitrate: int | None
    was_converted: bool | None
    original_filetype: str | None
    actual_filetype: str | None
    actual_min_bitrate: int | None
    slskd_filetype: str | None
    downloaded_label: str
    verdict: str
    summary: str
    failure_category: str | None
    analysis_error: str | None
    installed_path: str | None
    candidate_reference: str | None
    # Persisted QualityComparisonBasis as a plain dict (null on rows
    # predating the field) — the detail grid's "Compared" row.
    comparison_basis: dict[str, object] | None
    disambiguation_failure: str | None
    disambiguation_detail: str | None
    bad_extensions: list[str]
    wrong_match_triage_action: str | None
    wrong_match_triage_summary: str | None
    wrong_match_triage_reason: str | None
    wrong_match_triage_preview_verdict: str | None
    wrong_match_triage_preview_decision: str | None
    wrong_match_triage_stage_chain: list[str]
    wrong_match_triage_detail: str | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    existing_min_bitrate: int | None
    existing_avg_bitrate: int | None
    existing_median_bitrate: int | None
    existing_spectral_bitrate: int | None
    existing_spectral_grade: str | None
    spectral_attempted: bool | None
    spectral_error: str | None
    existing_spectral_attempted: bool | None
    existing_spectral_error: str | None
    existing_format: str | None
    source_format: str | None
    source_min_bitrate: int | None
    source_avg_bitrate: int | None
    source_median_bitrate: int | None
    target_contract_format: str | None
    legacy_projection_version: int | None
    materialized_format: str | None
    materialized_min_bitrate: int | None
    materialized_avg_bitrate: int | None
    materialized_median_bitrate: int | None
    final_format: str | None
    v0_probe_kind: str | None
    v0_probe_min_bitrate: int | None
    v0_probe_avg_bitrate: int | None
    v0_probe_median_bitrate: int | None
    existing_v0_probe_kind: str | None
    existing_v0_probe_min_bitrate: int | None
    existing_v0_probe_avg_bitrate: int | None
    existing_v0_probe_median_bitrate: int | None
    album_title: str
    artist_name: str
    mb_release_id: str | None
    request_status: str | None
    request_min_bitrate: int | None
    search_filetype_override: str | None
    source: str | None
    request_source: str | None = None
    youtube_metadata: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        return msgspec.to_builtins(self)


def build_download_history_rows(
    rows: Sequence[Mapping[str, object]],
) -> list[DownloadHistoryViewRow]:
    """Classify raw download_log rows into the shared detail-view contract."""
    return [build_download_history_row(row) for row in rows]


def classify_download_log_row(
    row: Mapping[str, object],
) -> ClassifiedDownloadLogRow:
    """Build the shared typed classification for one raw download_log row."""
    entry = LogEntry.from_row(dict(row))
    return ClassifiedDownloadLogRow(
        entry=entry,
        classified=classify_log_entry(entry),
    )


def build_download_history_row(
    row: Mapping[str, object],
) -> DownloadHistoryViewRow:
    """Build one detail-view history row from a raw download_log row."""
    classified_row = classify_download_log_row(row)
    entry = classified_row.entry
    classified = classified_row.classified
    merged: dict[str, object] = {
        **entry.to_json_dict(),
        **msgspec.to_builtins(classified),
    }
    merged["apply_beets_distance"] = _apply_beets_distance(
        merged.get("import_result"))
    return msgspec.convert(
        merged,
        type=DownloadHistoryViewRow,
        strict=True,
    )


def _apply_beets_distance(import_result: object) -> float | None:
    """Read #863's persisted apply-time distance off the row's JSONB."""
    if isinstance(import_result, str):
        try:
            import_result = json.loads(import_result)
        except ValueError:
            return None
    value = json_dict(import_result).get("apply_beets_distance")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)
