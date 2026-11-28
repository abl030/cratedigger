"""One display projection of a held upgrade, shared by every surface.

Issue #1445 item 3. Recents and History (``web/classify.py``), the Wrong
Matches worklist (``web/wrong_match_queue_view.py``) and ``pipeline-cli
show`` all render a held upgrade from the comparison stored on its
``download_log.validation_result`` (``upgrade_hold``), through this one
function, so the sentence and the table cannot drift between them. Nothing
is measured here; a row without stored evidence is simply not a held
upgrade.

Issue #1465 adds the second held kind, a folder-collision hold: the
sentence comes from the stored ``folder_collision`` evidence and there is
no track table.
"""

from __future__ import annotations

import msgspec

from lib.quality import FolderExclusivityGuardInfo
from lib.quality.installed_copy_guard import (
    InstalledCopyComparison,
    TrackLengthComparisonRow,
    held_upgrade_sentence,
)
from lib.track_length_shape import format_duration
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_match_policy import (
    is_held_scenario,
    is_upgrade_held_scenario,
)

_UNKNOWN = "—"


class HeldUpgradeTrackRow(msgspec.Struct, frozen=True, kw_only=True):
    """One release track, every length already written as m:ss."""

    track: str
    title: str
    release: str
    current: str
    new: str
    differs: bool


#: The two kinds of held folder (``lib.wrong_match_policy.HELD_SCENARIOS``).
HELD_KIND_UPGRADE = "upgrade"
HELD_KIND_FOLDER_COLLISION = "folder_collision"


class HeldUpgradeView(msgspec.Struct, frozen=True, kw_only=True):
    sentence: str
    rows: list[HeldUpgradeTrackRow]
    #: ``upgrade`` (#1445: track lengths disagree) or ``folder_collision``
    #: (#1465: the library folder is occupied; ``rows`` is empty).
    kind: str = HELD_KIND_UPGRADE
    #: The badge every surface shows for this hold.
    label: str = "Upgrade held"


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
    if not is_held_scenario(envelope.scenario):
        return None
    if not is_upgrade_held_scenario(envelope.scenario):
        if envelope.folder_collision is None:
            return None
        return HeldUpgradeView(
            sentence=folder_collision_sentence(envelope.folder_collision),
            rows=[],
            kind=HELD_KIND_FOLDER_COLLISION,
            label="Library folder occupied",
        )
    if envelope.upgrade_hold is None:
        return None
    return held_upgrade_view_from_comparison(envelope.upgrade_hold)


def folder_collision_sentence(guard: FolderExclusivityGuardInfo | None) -> str:
    """One operator sentence for a folder-collision hold (issue #1465)."""
    if guard is None or not guard.folder:
        return (
            "Not imported: its library folder already holds another album. "
            "Nothing was replaced."
        )
    holders = [
        f"{occupant.albumartist} - {occupant.album} "
        f"(beets id {occupant.beets_album_id})"
        for occupant in guard.occupants
    ]
    if guard.unowned_files:
        count = len(guard.unowned_files)
        holders.append(
            f"{count} file{'' if count == 1 else 's'} no album owns"
        )
    held_by = "; ".join(holders) or "other files"
    return (
        f"Not imported: the library folder {guard.folder} already holds "
        f"{held_by}. Nothing was replaced. Move or retag what is there, "
        "then retry the import."
    )
