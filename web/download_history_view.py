"""Typed download-log presentation helpers shared by detail and pipeline views."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import msgspec

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
    created_at: str | None
    beets_scenario: str | None
    beets_distance: float | None
    beets_detail: str | None
    soulseek_username: str | None
    error_message: str | None
    import_result: str | dict[str, object] | None
    validation_result: str | dict[str, object] | None
    filetype: str | None
    bitrate: int | None
    was_converted: bool | None
    original_filetype: str | None
    actual_filetype: str | None
    actual_min_bitrate: int | None
    slskd_filetype: str | None
    slskd_bitrate: int | None
    downloaded_label: str
    verdict: str
    disambiguation_failure: str | None
    disambiguation_detail: str | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    existing_min_bitrate: int | None
    existing_spectral_bitrate: int | None
    album_title: str
    artist_name: str
    mb_release_id: str | None
    request_status: str | None
    request_min_bitrate: int | None
    search_filetype_override: str | None
    source: str | None

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))


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
    return msgspec.convert(
        {
            **entry.to_json_dict(),
            "downloaded_label": classified.downloaded_label,
            "verdict": classified.verdict,
            "disambiguation_failure": classified.disambiguation_failure,
            "disambiguation_detail": classified.disambiguation_detail,
        },
        type=DownloadHistoryViewRow,
    )
