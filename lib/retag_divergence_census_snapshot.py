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

import json
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
    prior file at ``path`` untouched.

    Encodes via stdlib ``json.dumps`` (default ``ensure_ascii=True``),
    not ``msgspec.json.encode`` (#1142 review N2): a Beets item path
    decoded from non-UTF-8 filesystem bytes carries a lone surrogate
    codepoint (``os.fsdecode``'s ``surrogateescape`` shape), which
    ``msgspec.json.encode`` refuses to encode at all (strict UTF-8) —
    the exact same string the pre-existing CLI/API JSON output (also
    stdlib ``json.dumps``) already tolerates by escaping it to a plain
    ASCII ``\\udcXX`` sequence. ``msgspec.to_builtins`` only converts
    the Struct to plain dicts/lists/strings — it never touches string
    content — so the round trip stays exactly as strict about
    STRUCTURE as the previous ``msgspec.json`` calls, just tolerant of
    this one real-world string shape.
    """
    data = json.dumps(msgspec.to_builtins(snapshot)).encode("ascii")
    _atomic_write_bytes(path, data)


def read_retag_divergence_census_snapshot(
    path: str,
) -> RetagDivergenceCensusSnapshot | None:
    """Read the persisted snapshot, or ``None`` if the daily census has
    never published one yet — a real, honest state, not an error.

    Malformed content (a bit-rotted or hand-edited file — atomic
    publication rules out a partial write from this module itself) can
    raise ``UnicodeDecodeError`` (not ASCII), ``json.JSONDecodeError``
    (not valid JSON), or ``msgspec.ValidationError`` (valid JSON, wrong
    shape) — callers decide how to present any of those distinctly from
    "missing". Decodes via stdlib ``json.loads`` then ``msgspec.convert``
    for strict structural validation — the tolerant-encode counterpart
    of :func:`write_retag_divergence_census_snapshot`; see its docstring
    for why (#1142 review N2). ``msgspec.convert`` validates the
    already-parsed Python object's shape/types exactly as strictly as
    ``msgspec.json.decode`` did — it just never re-checks the string
    VALUES for UTF-8 encodability, since they are already live Python
    ``str`` objects at that point.
    """
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except FileNotFoundError:
        return None
    decoded = json.loads(data.decode("ascii"))
    return msgspec.convert(decoded, type=RetagDivergenceCensusSnapshot)


__all__ = [
    "RETAG_DIVERGENCE_CENSUS_SNAPSHOT_FILENAME",
    "RetagDivergenceCensusSnapshot",
    "read_retag_divergence_census_snapshot",
    "retag_divergence_census_snapshot_path",
    "write_retag_divergence_census_snapshot",
]
