"""Typed wrapper around ``beet remove`` subprocess ops (issue #133).

Single source of truth for invoking beets destructive commands at the
subprocess level. Extracted to unify the five+ ad-hoc callsites that
PR #131 spread across the codebase before the Cratedigger-owned
replacement state machine was removed. Every new callsite that touches
``beet remove`` must route through this module.

A contract test (``tests/test_beets_album_op.py::TestBeetOpArgvIsCentralised``)
greps the repo at test time and fails if any file outside this module
constructs ``["beet", "remove", ...]`` argv. The grep is the enforcement
mechanism — nothing stops you from writing raw argv elsewhere, but the
suite fails if you do.

(Previously this module also wrapped ``beet move`` via ``move_album``;
the legacy replacement cleanup that called it was removed in
``01ede8d``, and ``move_album`` was retired here in the dead-code audit
that opened #352.)

Invariants this module enforces by construction (callers cannot bypass
without rewriting the op). Note these are *structural* guarantees —
``album_id`` is just a Python ``int``, so ``BeetsAlbumHandle(album_id=0)``
or ``-1`` would still construct; that's a convention for callers, not
a type-level constraint:

1. **Album-mode (``-a``) is mandatory.** Every argv built by
   ``_run_beet_op`` uses ``[beet, "-P", "importsource", verb, "-a",
   "-f"]``. Without ``-a``
   the ``id:<N>`` selector would be interpreted against ``items.id``
   (a single track row in a separate auto-increment namespace), not
   ``albums.id``. PR #131 round 2 P1 caught item-mode silently
   matching unrelated tracks.

2. **Primary-key-scoped selectors for id-based ops.** ``remove_album``
   takes a ``BeetsAlbumHandle`` and always emits ``id:<album_id>``. The
   PK selector is a ``SELECT ... WHERE id = ?`` which cannot match
   cross-MBID siblings (the Palo Santo data-loss root cause). Arbitrary
   selectors route through ``remove_by_selector`` where the caller is
   explicitly opting out of PK narrowing.

3. **Source-agnostic.** ``mb_albumid`` is empty for Discogs rows and
   ``discogs_albumid`` is empty for MB rows; ``albums.id`` is the one
   identifier always populated. PR #131 round 3 P3 and round 4 P3
   flagged the earlier MBID-based moves silently no-oping for Discogs.

4. **Never raise.** Every subprocess invocation is wrapped in
   try/except for ``TimeoutExpired`` and ``OSError`` and every non-zero
   rc becomes a typed failure. Callers inspect the returned
   ``BeetsOpResult`` — they never parse stderr or catch ``sp`` errors.

For arbitrary-selector removals (ban-source cleanup where the caller
doesn't know the album id but has an mb_albumid / discogs_albumid),
``remove_by_selector`` is the low-level primitive. Prefer ``remove_album``
when the album id is known — the PK-scoped selector is narrower.
"""

from __future__ import annotations

import logging
import subprocess as sp
from dataclasses import dataclass
from typing import Literal

import msgspec

from lib.beets_config_contract import (
    BeetsConfigError,
    validate_beets_config,
    validate_beets_plugins_loaded,
)

log = logging.getLogger("cratedigger")


# Default timeout. ``remove`` is a quick DB delete; callers may override.
DEFAULT_REMOVE_TIMEOUT = 30  # seconds


BeetsOpFailureReason = Literal["timeout", "nonzero_rc", "exception"]


class BeetsOpFailure(msgspec.Struct, frozen=True):
    """Why a single ``beet remove`` invocation did not exit cleanly.

    ``reason`` is a coarse Literal tag — callers and downstream JSONB
    audit consumers can classify failures at a glance without parsing
    ``detail``. ``detail`` is a short human-readable string for logs
    and the audit trail; do not parse it.

    ``selector`` is the argv selector string (``id:42``,
    ``mb_albumid:<uuid>``, ``discogs_albumid:<id>``) so downstream logs
    and the web UI Recents tab can disambiguate failures across
    multi-selector loops (e.g. release_cleanup iterating both
    ``mb_albumid`` and ``discogs_albumid``).

    Defaulting ``selector`` to ``""`` keeps JSON round-trip backwards
    compatible with old ``PostflightInfo.disambiguation_failure`` rows
    that predate the field (written before this module collapsed
    ``DisambiguationFailure`` into ``BeetsOpFailure``).

    Wire-boundary type per ``.claude/rules/code-quality.md`` §
    "Wire-boundary types" — crosses JSONB as a nested value inside
    ``PostflightInfo.disambiguation_failure`` and as route response
    payload in ``web/routes/pipeline.py``. Encoded via
    ``msgspec.json.encode`` / ``msgspec.to_builtins``; decoded via
    ``msgspec.convert`` — symmetric.
    """
    reason: BeetsOpFailureReason
    detail: str
    selector: str = ""


@dataclass(frozen=True)
class BeetsAlbumHandle:
    """Source-agnostic handle to one row in ``beets.albums``.

    ``album_id`` is the beets numeric primary key — always populated
    (SQLite auto-increment), unique by construction, narrow enough that
    the ``id:<N>`` selector cannot reach a sibling pressing.

    One-field dataclass kept as a distinct type rather than a bare
    ``int`` so callsites are self-documenting (``BeetsAlbumHandle(
    album_id=N)`` vs ``remove_album(N)``) and future additions — e.g.
    a debug label, a request_id backref — can land without breaking
    callsite signatures. Earlier drafts carried a ``release_id: str``
    field for logs; nothing read it, so it was removed (YAGNI).
    """
    album_id: int


@dataclass(frozen=True)
class BeetsOpResult:
    """Outcome of a single ``remove_album`` call.

    Never raised — callers inspect and branch.

    - ``success``: ``True`` iff the subprocess exited rc=0 with no
      raised exception. ``failure`` is ``None`` in that case.
    - ``failure``: the typed failure when ``success=False``.
    """
    success: bool
    failure: BeetsOpFailure | None = None


def _run_beet_op(
    verb: Literal["remove"],
    selector: str,
    *,
    delete_files: bool = False,
    timeout: int,
) -> BeetsOpFailure | None:
    """Run one forced, album-scoped ``beet remove`` invocation. Never raises.

    Internal primitive; ``remove_album`` and ``remove_by_selector`` are
    the public entry points. Captures every
    fragile failure mode (``TimeoutExpired``, ``OSError`` from a
    missing ``beet`` binary, non-zero returncode) and classifies each
    into a typed ``BeetsOpFailure``. Returns ``None`` on clean exit.

    The ``-a`` flag is mandatory — see the module docstring, invariant 1.
    """
    # Deferred imports break a top-level cycle. Exact path:
    #   lib.beets_album_op ── top-level import ──► lib.util
    #   lib.util           ── mid-body import ───► lib.quality
    #   lib.quality        ── top-level import ──► lib.beets_album_op
    #                        (DisambiguationFailure alias for BeetsOpFailure)
    # If this module imported ``lib.util`` at top level, loading
    # ``harness/import_one.py`` would trigger the chain and hit
    # beets_album_op mid-init (BeetsOpFailure not defined yet).
    # Deferring the ``lib.util`` import to call time keeps the
    # top-level import graph acyclic. Python caches module imports so
    # the per-call cost is a dict lookup.
    from lib.util import beet_bin, beets_subprocess_env

    try:
        # Config-resolution (tier-2 U5): both helpers raise an actionable
        # RuntimeError when the beets runtime keys are unset. Fold that
        # into the typed failure so this function's "never raises"
        # contract holds for callers built around BeetsOpFailure.
        beet = beet_bin()
        beets_env = beets_subprocess_env()
        configured_plugins = validate_beets_config(beets_env["BEETSDIR"])
        validate_beets_plugins_loaded(
            beet, beets_env, configured_plugins, timeout=timeout,
        )
    except (RuntimeError, BeetsConfigError) as exc:
        msg = str(exc)
        log.warning("beets_album_op: beet %s %s %s", verb, selector, msg)
        return BeetsOpFailure(reason="exception", detail=msg, selector=selector)

    # ``importsource.suggest_removal`` prompts once per removed item and does
    # not honor Beets' command-level ``--force`` flag. Cratedigger's delete
    # authority covers the managed library copy only, never the plugin's
    # separately recorded import source, so the plugin is disabled for this
    # operation. Every module-rendered production plugin remains loaded.
    argv: list[str] = [beet, "-P", "importsource", verb, "-a", "-f"]
    if verb == "remove" and delete_files:
        argv.append("-d")
    argv.append(selector)

    try:
        # ``--force`` is Beets' supported noninteractive contract. Closing
        # stdin is independent defense: plugin initialization must never
        # consume an operator keystroke or a parent's protocol payload.
        proc = sp.run(
            argv,
            capture_output=True, text=True, errors="replace", timeout=timeout,
            env=beets_env,
            stdin=sp.DEVNULL,
        )
    except sp.TimeoutExpired as exc:
        msg = f"timed out after {exc.timeout}s"
        log.warning("beets_album_op: beet %s %s %s", verb, selector, msg)
        return BeetsOpFailure(reason="timeout", detail=msg, selector=selector)
    except OSError as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning(
            "beets_album_op: beet %s %s raised %s", verb, selector, msg)
        return BeetsOpFailure(
            reason="exception", detail=msg, selector=selector)

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip().splitlines()
        last = stderr[-1] if stderr else ""
        detail = (f"rc={proc.returncode}: {last}"
                  if last else f"rc={proc.returncode}")
        log.warning(
            "beets_album_op: beet %s %s exited %d: %s",
            verb, selector, proc.returncode, detail)
        return BeetsOpFailure(
            reason="nonzero_rc", detail=detail, selector=selector)

    return None


def remove_album(
    handle: BeetsAlbumHandle,
    *,
    delete_files: bool = True,
    timeout: int = DEFAULT_REMOVE_TIMEOUT,
) -> BeetsOpResult:
    """Remove one beets album by numeric primary key.

    Runs a forced, album-scoped ``beet remove [-d] id:<album_id>``. Never raises — any
    subprocess failure is surfaced as a typed ``BeetsOpResult`` with
    ``success=False`` and a populated ``failure``.

    The ``id:<N>`` selector is a ``SELECT ... WHERE id = ?`` — a beets
    numeric PK is unique by construction, so this cannot match any
    album but the one named. Safe to use after a successful upgrade
    import when the old and new albums briefly coexist.

    ``delete_files=True`` (default) deletes the tagged files on disk.
    ``delete_files=False`` untags the album but leaves files — not
    used by current production callers.
    """
    selector = f"id:{handle.album_id}"
    failure = _run_beet_op(
        "remove", selector, delete_files=delete_files, timeout=timeout)
    return BeetsOpResult(success=failure is None, failure=failure)


def remove_by_selector(
    selector: str,
    *,
    timeout: int = DEFAULT_REMOVE_TIMEOUT,
) -> BeetsOpFailure | None:
    """Low-level primitive: forced album ``beet remove -d <selector>``.

    For callsites that iterate arbitrary selectors (``mb_albumid:X``,
    ``discogs_albumid:Y``) because the album id is not known up front
    — the ban-source cleanup path in ``lib.release_cleanup`` is the
    canonical caller. Prefer ``remove_album(handle)`` whenever the
    album id is available: the ``id:<N>`` selector is narrower and
    cannot accidentally match siblings.

    ``-d`` (delete files) is always on: every caller's intent is
    "remove from beets AND delete the tagged files" (ban-source
    cleanup). An untag-only selector-based remove has no production
    use case today; if one appears, add the flag then.

    Returns ``None`` on clean exit or a typed ``BeetsOpFailure``.
    """
    return _run_beet_op(
        "remove", selector, delete_files=True, timeout=timeout)
