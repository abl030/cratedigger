"""Typed download-history rows for detail views."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from web.classify import LogEntry, classify_log_entry


class DownloadHistoryViewRow(msgspec.Struct, frozen=True):
    """Frontend contract shared by detail-view download-history panels."""

    id: int
    request_id: int
    outcome: str
    created_at: str | None
    soulseek_username: str | None
    downloaded_label: str
    verdict: str
    beets_scenario: str | None
    beets_distance: float | None
    spectral_grade: str | None
    spectral_bitrate: int | None
    existing_min_bitrate: int | None
    existing_spectral_bitrate: int | None

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], msgspec.to_builtins(self))


def build_download_history_rows(
    rows: Sequence[Mapping[str, object]],
) -> list[DownloadHistoryViewRow]:
    """Classify raw download_log rows into the shared detail-view contract."""
    items: list[DownloadHistoryViewRow] = []
    for row in rows:
        entry = LogEntry.from_row(dict(row))
        classified = classify_log_entry(entry)
        items.append(msgspec.convert(
            {
                "id": entry.id,
                "request_id": entry.request_id,
                "outcome": entry.outcome,
                "created_at": entry.created_at,
                "soulseek_username": entry.soulseek_username,
                "downloaded_label": classified.downloaded_label,
                "verdict": classified.verdict,
                "beets_scenario": entry.beets_scenario,
                "beets_distance": entry.beets_distance,
                "spectral_grade": entry.spectral_grade,
                "spectral_bitrate": entry.spectral_bitrate,
                "existing_min_bitrate": entry.existing_min_bitrate,
                "existing_spectral_bitrate": entry.existing_spectral_bitrate,
            },
            type=DownloadHistoryViewRow,
        ))
    return items
