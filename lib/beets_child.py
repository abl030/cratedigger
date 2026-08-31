"""One owner for pinned-Beets child-process mechanics (#1278 item 4).

Every run-to-completion Beets mutation child Cratedigger launches — the
exact-album delete (``lib/beets_delete.py``), the import-time merge retag
(``lib/beets_retag.py``), and the one-album file-tag sync
(``lib/beets_tag_sync.py``) — starts the same way: resolve the deployment
environment and pinned interpreter through
``lib/util.py::beets_subprocess_env`` (the single source of truth for how a
beets subprocess finds its config and interpreter), refuse loudly when the
interpreter is unconfigured, and run ``[<beets python>, *argv_tail]`` with
captured output under a lane-supplied timeout. This module owns exactly
that shared spawn; each lane still owns its own argv TAIL (its flags are
lane policy, individually load-bearing) and its own evidence mechanism.

The streaming harness sessions — validation in ``lib/beets.py`` and the
import in ``harness/import_one.py`` — spawn ``run_beets_harness.sh``
instead, whose bash performs the same interpreter refusal before ``exec``;
their interactive stdin/stdout protocol is not a run-to-completion child
and stays with them.

**The exit code is not evidence.** A beets query matching nothing exits 1
(``UserError``); a query that matches but changes nothing exits 0 — and
either way an exit code read against a shared SQLite file another process
can concurrently mutate is never itself an observation of the end state.
Each lane decides "did the mutation land" from its own re-read of the
world: the delete from its typed outcome frame, the retag from the re-read
library, the tag sync from the re-read file tags. Nothing in this module
decides anything; it launches, captures, and reports.

The four Beets mutation lanes stay four (CLAUDE.md § Decision
architecture); this module adds no lane — only the mechanics they share.
Module-top imports stay stdlib-only so ``harness/delete_album.py`` (which
runs INSIDE the pinned beets environment and imports ``lib.beets_delete``)
can keep importing its lane without dragging Cratedigger's config stack
into that environment; ``beets_subprocess_env`` is imported at call time
for the same reason.
"""

from __future__ import annotations

import subprocess as sp
from collections.abc import Callable, Sequence
from dataclasses import dataclass

#: The injectable child runner every lane forwards to
#: :func:`run_pinned_beets_child` — the one leaf-seam DI point for these
#: subprocesses. Signature-compatible with ``sp.run``.
type SubprocessRunFn = Callable[..., sp.CompletedProcess[bytes]]


@dataclass(frozen=True)
class BeetsChildRun:
    """What one pinned-Beets child invocation reported — diagnostic detail
    only, never a decision input (see the module docstring: the exit code
    is not evidence)."""

    returncode: int
    stdout: str
    stderr: str

    @classmethod
    def from_completed(cls, proc: sp.CompletedProcess[bytes]) -> BeetsChildRun:
        """Decode a captured child for diagnostics. ``errors="replace"``
        because non-UTF-8 bytes in a child's streams (CP1252-tagged
        metadata echoed by beets) must never raise during capture."""
        return cls(
            returncode=proc.returncode,
            stdout=proc.stdout.decode("utf-8", errors="replace"),
            stderr=proc.stderr.decode("utf-8", errors="replace"),
        )


def run_pinned_beets_child(
    argv_tail: Sequence[str],
    *,
    timeout: int,
    input_bytes: bytes | None = None,
    runner: SubprocessRunFn = sp.run,
) -> sp.CompletedProcess[bytes]:
    """Run ``[<beets python>, *argv_tail]`` in the deployment-supplied
    Beets runtime with captured output.

    ``python -m beets`` tails are how the lanes reach the beets CLI: a
    ``beet`` binary found on this process's PATH would silently be
    whatever beets the invoking user happens to have, never the pinned
    runtime. Raises ``RuntimeError`` when the environment cannot name a
    config dir or interpreter (``beets_subprocess_env``'s own fail-closed
    refusals), and propagates launch/timeout failures unconverted — each
    lane owns its failure typing, exactly as it owns its evidence.
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env.get("CRATEDIGGER_BEETS_PYTHON", "")
    if not python:
        raise RuntimeError("CRATEDIGGER_BEETS_PYTHON is not configured")
    argv = [python, *argv_tail]
    if input_bytes is None:
        return runner(argv, capture_output=True, timeout=timeout, env=env)
    return runner(
        argv, input=input_bytes, capture_output=True, timeout=timeout, env=env,
    )


__all__ = [
    "BeetsChildRun",
    "SubprocessRunFn",
    "run_pinned_beets_child",
]
