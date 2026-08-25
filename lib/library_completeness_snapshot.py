"""Atomic persisted snapshot for the daily library completeness census."""
from __future__ import annotations

import json
import os

import msgspec

from lib.library_completeness import CompletenessReport
from lib.sidecar_service import _atomic_write_bytes

LIBRARY_COMPLETENESS_SNAPSHOT_FILENAME = "library-completeness.json"

#: Trigger file watched by the module's
#: ``cratedigger-library-completeness-census.path`` unit: its existence
#: starts the same-named census oneshot, whose ``ExecStartPre`` removes
#: it. Written by the operator-facing refresh action (web route +
#: ``pipeline-cli library-census-refresh`` relay) so a census can be
#: forced without waiting for the daily timer — the unit's ``ExecStart``
#: stays the ONE census execution path.
LIBRARY_COMPLETENESS_TRIGGER_FILENAME = "library-completeness-census.trigger"


class LibraryCompletenessSnapshot(msgspec.Struct, frozen=True):
    generated_at: str
    duration_seconds: float
    report: CompletenessReport


def library_completeness_snapshot_path(var_dir: str) -> str:
    return os.path.join(var_dir, LIBRARY_COMPLETENESS_SNAPSHOT_FILENAME)


def write_library_completeness_snapshot(
    path: str, snapshot: LibraryCompletenessSnapshot,
) -> None:
    # JSON's ASCII form also preserves any surrogateescaped Beets pathname.
    _atomic_write_bytes(path, json.dumps(msgspec.to_builtins(snapshot)).encode("ascii"))


def read_library_completeness_snapshot(
    path: str,
) -> LibraryCompletenessSnapshot | None:
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except FileNotFoundError:
        return None
    return msgspec.convert(json.loads(data.decode("ascii")), type=LibraryCompletenessSnapshot)


def library_completeness_trigger_path(var_dir: str) -> str:
    return os.path.join(var_dir, LIBRARY_COMPLETENESS_TRIGGER_FILENAME)


class CensusTriggerResult(msgspec.Struct, frozen=True):
    """Wire-facing outcome of a census refresh request."""

    outcome: str


def request_library_completeness_census(var_dir: str) -> CensusTriggerResult:
    """Request an out-of-schedule census run by writing the trigger file.

    Idempotent: a second request while one is pending rewrites the same
    file. systemd's ``PathExists=`` semantics give the useful behavior
    for a request made DURING a run: the path unit re-triggers after the
    active run deactivates, so the next snapshot reflects post-request
    state. Raises ``OSError`` when the state dir is unwritable — the
    callers map that to 503/exit 5.
    """
    path = library_completeness_trigger_path(var_dir)
    with open(path, "w", encoding="ascii") as fh:
        fh.write("requested\n")
    return CensusTriggerResult(outcome="requested")
