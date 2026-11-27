"""One display projection of a held upgrade, shared by every surface.

Issue #1445 item 3. Recents and History (``web/classify.py``), the Wrong
Matches worklist (``web/wrong_match_queue_view.py``) and ``pipeline-cli
show`` all render a held upgrade from the comparison stored on its
``download_log.validation_result`` (``upgrade_hold``), through this one
function, so the sentence and the table cannot drift between them. Nothing
is measured here; a row without stored evidence is simply not a held
upgrade.
"""

from __future__ import annotations

import msgspec

from lib.quality.installed_copy_guard import (
    InstalledCopyComparison,
    TrackLengthComparisonRow,
    held_upgrade_sentence,
)
from lib.track_length_shape import format_duration
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_policy import is_upgrade_held_scenario

_UNKNOWN = "—"


class HeldUpgradeTrackRow(msgspec.Struct, frozen=True, kw_only=True):
    """One release track, every length already written as m:ss."""

    track: str
    title: str
    release: str
    current: str
    new: str
    differs: bool


class HeldUpgradeView(msgspec.Struct, frozen=True, kw_only=True):
    sentence: str
    rows: list[HeldUpgradeTrackRow]


def _length(seconds: float | None) -> str:
    return _UNKNOWN if seconds is None else format_duration(seconds)


def _track_label(row: TrackLengthComparisonRow, *, multi_disc: bool) -> str:
    if row.medium_index is None:
        return _UNKNOWN
    if multi_disc and row.medium is not None:
        return f"{row.medium}-{row.medium_index}"
    return str(row.medium_index)


def held_upgrade_view_from_comparison(
    comparison: InstalledCopyComparison,
) -> HeldUpgradeView:
    multi_disc = len({
        row.medium for row in comparison.rows if row.medium is not None
    }) > 1
    return HeldUpgradeView(
        sentence=held_upgrade_sentence(comparison),
        rows=[
            HeldUpgradeTrackRow(
                track=_track_label(row, multi_disc=multi_disc),
                title=row.title,
                release=_length(row.declared_seconds),
                current=_length(row.installed_seconds),
                new=_length(row.candidate_seconds),
                differs=row.tripped,
            )
            for row in comparison.rows
        ],
    )


def held_upgrade_view(validation_result: object) -> HeldUpgradeView | None:
    """The held-upgrade display for one stored audit row, or ``None``."""
    try:
        envelope = decode_validation_envelope(validation_result)
    except (msgspec.ValidationError, ValueError):
        return None
    if not is_upgrade_held_scenario(envelope.scenario):
        return None
    if envelope.upgrade_hold is None:
        return None
    return held_upgrade_view_from_comparison(envelope.upgrade_hold)
