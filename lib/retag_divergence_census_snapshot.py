"""Persisted daily retag-divergence census snapshot (#1142).

The whole-library census (``lib/retag_divergence_audit.py``) scans the
entire Beets library — ~93,700 files / ~200s measured live — far too
expensive to run at dashboard render or normal web API request time. A
daily oneshot (``scripts/run_retag_divergence_census.py``) runs the
unbounded whole-library scan and publishes exactly one
:class:`RetagDivergenceCensusSnapshot` here; the dashboard route and the
per-album recheck both read this file, never the whole-library scan
itself.

Publication is atomic (same-directory temp file + ``os.replace``, reusing
``lib.sidecar_service``'s already-proven helper): a reader never observes
a partially written file, and a run that fails before completing this
call leaves any prior snapshot untouched — the daily writer's own
contract for acceptance criterion 1 ("preserves a prior valid snapshot
when a run fails").

Read-only: nothing in this module mutates Beets, PostgreSQL, or the
filesystem beyond the one snapshot file it owns.
"""

from __future__ import annotations

import os

import msgspec

from lib.retag_divergence_audit import RetagDivergenceReport
from lib.sidecar_service import _atomic_write_bytes

#: Snapshot filename inside the mutable runtime state directory
#: (``CratediggerConfig.var_dir`` — the same directory the lock file and
#: processing root live in; ``cfg.stateDir`` in the deployed NixOS module).
RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME = "retag-divergence-census.json"


def retag_divergence_census_snapshot_path(var_dir: str) -> str:
    """The one canonical snapshot path, derived from the runtime state
    directory — shared by the daily writer and every reader so neither
    can drift onto a different location."""
    return os.path.join(var_dir, RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME)


class RetagDivergenceCensusSnapshot(msgspec.Struct, frozen=True):
    """One published run of the daily whole-library census: the full
    report plus when it ran and how long it took."""

    #: ISO 8601 UTC timestamp of when this run started.
    generated_at: str
    duration_seconds: float
    report: RetagDivergenceReport


def write_retag_divergence_census_snapshot(
    path: str, snapshot: RetagDivergenceCensusSnapshot,
) -> None:
    """Publish ``snapshot`` at ``path`` in one atomic same-directory
    rename. A reader never observes a partially written file; a caller
    whose write raises (this propagates, never swallowed) has left any
    prior file at ``path`` untouched."""
    _atomic_write_bytes(path, msgspec.json.encode(snapshot))


def read_retag_divergence_census_snapshot(
    path: str,
) -> RetagDivergenceCensusSnapshot | None:
    """Read the persisted snapshot, or ``None`` if the daily census has
    never published one yet — a real, honest state, not an error.

    Malformed content (a bit-rotted or hand-edited file — atomic
    publication rules out a partial write from this module itself)
    raises ``msgspec.DecodeError``/``msgspec.ValidationError``; callers
    decide how to present that distinctly from "missing"."""
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except FileNotFoundError:
        return None
    return msgspec.json.decode(data, type=RetagDivergenceCensusSnapshot)


__all__ = [
    "RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME",
    "RetagDivergenceCensusSnapshot",
    "read_retag_divergence_census_snapshot",
    "retag_divergence_census_snapshot_path",
    "write_retag_divergence_census_snapshot",
]
