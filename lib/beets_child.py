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
instead, whose bash performs the same interpreter refusal before ``exec``.
Their one shared argv shape is :func:`harness_session_argv` (the two
sessions differ only by ``--pretend``), and the validation session's
spawner is :func:`spawn_harness_session` behind the narrow
:class:`HarnessSession` protocol — the lane-A injection seam.
``import_one.py`` keeps its own spawn (it needs ``start_new_session`` and
passes its snapshotted Beets authority explicitly); the interactive
stdin/stdout protocol itself stays with each session's driver.

**The exit code is never success evidence.** For the two ``python -m
beets`` lanes, a query matching nothing exits 1 (``UserError``) while a
query that matches but changes nothing exits 0 — and either way an exit
code read against a shared SQLite file another process can concurrently
mutate is not an observation of the end state; those lanes decide "did the
mutation land" from their own re-read of the world (the retag from the
re-read library, the tag sync from the re-read file tags) and keep the run
record as diagnostics only. The delete lane's child is Cratedigger's own
``harness/delete_album.py``, which answers through a typed outcome frame
on stdout; a nonzero exit there IS its refusal signal — ``run_beets_delete``
maps it to a failed outcome with the album still present, conservative and
never a claim the deletion happened. Nothing in this module decides
anything; it launches, captures, and reports.

The four Beets mutation lanes stay four (CLAUDE.md § Decision
architecture); this module adds no lane — only the mechanics they share.
The module top stays deliberately light (stdlib only today), and
``beets_subprocess_env`` is imported inside the two spawning functions,
continuing the lanes' own convention. Neither is load-bearing: the pinned interpreter
in this deployment is Cratedigger's own Python environment
(``nix/module.nix`` renders ``[Beets] python`` from the same ``pythonEnv``
the pipeline runs on), so ``harness/delete_album.py``'s in-child import of
``lib.beets_delete`` — and through it this module — resolves either way.
"""

from __future__ import annotations

import subprocess as sp
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Protocol

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
    # NOT Sequence[str]: a bare str satisfies Sequence[str] under strict
    # Pyright and would splat into per-character argv — the lanes' "separate
    # argv elements, never one joined string" discipline must be a type
    # error, not a runtime surprise.
    argv_tail: list[str] | tuple[str, ...],
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


class HarnessStdin(Protocol):
    """What a session driver writes decisions through."""

    def write(self, data: str, /) -> int: ...
    def flush(self) -> None: ...


class HarnessStdout(Protocol):
    """What a session driver reads protocol lines from."""

    def __iter__(self) -> Iterator[str]: ...


class HarnessStderr(Protocol):
    """What a session driver harvests diagnostics from."""

    def read(self) -> str: ...


class HarnessSession(Protocol):
    """The slice of a text-mode ``sp.Popen[str]`` a streaming harness
    session actually drives — the lane-A injection seam's return type.
    ``sp.Popen[str]`` satisfies it structurally; tests satisfy it with a
    small typed fake instead of module-attribute patching."""

    @property
    def stdin(self) -> HarnessStdin | None: ...
    @property
    def stdout(self) -> HarnessStdout | None: ...
    @property
    def stderr(self) -> HarnessStderr | None: ...
    def kill(self) -> None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


#: The injectable spawner for a streaming harness session — a
#: definition-time default on ``lib/beets.py``'s validation entry points,
#: so tests inject a replacement and never patch a module binding.
type HarnessSpawnFn = Callable[[list[str]], HarnessSession]


def harness_session_argv(
    harness_path: str,
    *,
    mb_release_id: str,
    album_path: str,
    # Deliberately NO default: this is the one flag separating a dry run
    # from a real Beets import, so a forgotten kwarg must be a TypeError,
    # never a silent real import (review round, reader finding 5).
    pretend: bool,
    preserve_discogs_flat_subtracks: bool = False,
) -> list[str]:
    """The one argv shape for a streaming harness session
    (``run_beets_harness.sh``): the validation session (``--pretend``) and
    the real import differ ONLY by that flag. ``--noincremental`` is
    unconditional — a session that silently skipped a previously-seen
    directory would offer no match and read as ``no_choose_match``.
    """
    argv = [harness_path]
    if pretend:
        argv.append("--pretend")
    argv.append("--noincremental")
    if preserve_discogs_flat_subtracks:
        argv.append("--preserve-discogs-flat-subtracks")
    argv.extend(["--search-id", mb_release_id, album_path])
    return argv


def spawn_harness_session(argv: list[str]) -> HarnessSession:
    """Production spawner for the validation harness session: all three
    streams piped, text mode with ``errors="replace"`` (non-UTF-8 bytes in
    harness output must never raise mid-read), environment from
    ``beets_subprocess_env`` — the Blueline Medic 0-candidates incident
    class is a harness child resolving beets config from the wrong
    environment."""
    from lib.util import beets_subprocess_env

    return sp.Popen(
        argv, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE,
        text=True, errors="replace", env=beets_subprocess_env(),
    )


__all__ = [
    "BeetsChildRun",
    "HarnessSession",
    "HarnessSpawnFn",
    "HarnessStderr",
    "HarnessStdin",
    "HarnessStdout",
    "SubprocessRunFn",
    "harness_session_argv",
    "run_pinned_beets_child",
    "spawn_harness_session",
]
