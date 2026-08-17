"""Atomic persisted snapshot for the daily library completeness census."""
from __future__ import annotations

import json
import os

import msgspec

from lib.library_completeness import CompletenessReport
from lib.sidecar_service import _atomic_write_bytes

LIBRARY_COMPLETENESS_SNAPSHOT_FILENAME = "library-completeness.json"


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
