"""Atomic 'remove from beets + reset pipeline state' primitive (issue #121).

Before this module, the ban-source route hand-rolled the coupling:
choose the right selector(s) based on ID shape, run ``beet remove -d``,
verify the album is absent, then call ``clear_on_disk_quality_fields``.
Every Codex round on PR #119 surfaced one of those steps being skipped
— a selector forgotten, a clear missed when the first-pass query
missed a legacy Discogs album, a cleared-but-not-removed race in the
'already gone' path.

The function below is the only way to couple the two sides. Given a
``release_id`` (UUID or Discogs numeric) it:

1. Calls ``BeetsDB.locate(release_id)`` to enumerate every selector
   the ID could live under (one for UUIDs, two for Discogs numerics
   covering the new + legacy layouts).
2. If the album is exact-present, runs ``beet remove -d`` for EVERY
   selector. Hitting a selector that doesn't hold the album is a
   harmless no-op; skipping the one that does would silently leave
   the banned copy on disk.
3. Re-queries ``locate`` to confirm the album is absent.
4. If absent (whether this call removed it or a prior ``beet rm``
   did), clears the pipeline DB's on-disk quality fields so stale
   ``current_spectral_*`` / ``imported_path`` / ``verified_lossless``
   can't mislead downstream consumers.

Issue #123 PR B: each ``sp.run`` is now wrapped in a try/except that
catches ``TimeoutExpired``, non-zero exit codes, and any ``OSError``
(e.g. ``beet`` missing from PATH). The loop always attempts every
selector, and per-selector failures are surfaced via
``ReleaseCleanupResult.selector_failures`` so the caller can tell
partial failure from a clean run. Before this change, a
``TimeoutExpired`` on selector 1 escaped the loop and left selector 2
untried — *after* the ban-source caller had already committed the
denylist row, leaving the banned copy on disk with no recovery path.
"""

from __future__ import annotations

import logging
import subprocess as sp
from dataclasses import dataclass
from typing import Literal, TYPE_CHECKING

from lib.util import beets_subprocess_env

if TYPE_CHECKING:
    from lib.beets_db import BeetsDB
    from lib.pipeline_db import PipelineDB


log = logging.getLogger("soularr")


SelectorFailureReason = Literal["timeout", "nonzero_rc", "exception"]


@dataclass(frozen=True)
class SelectorFailure:
    """One ``beet remove -d`` attempt that didn't cleanly exit.

    ``reason`` is a coarse tag so callers (including the web UI) can
    classify at a glance without parsing ``detail`` strings. Keep the
    set closed — see ``SelectorFailureReason``. ``detail`` is a short
    human-readable string for logs and debugging; do not parse it.
    """
    selector: str
    reason: SelectorFailureReason
    detail: str


@dataclass(frozen=True)
class ReleaseCleanupResult:
    """Outcome of a ``remove_and_reset_release`` call.

    - ``beets_removed``: the album was present before the call AND is
      absent afterward — i.e. this call is responsible for its removal.
      A prior out-of-band ``beet rm`` returns False here; the pipeline
      DB is still cleared if the album is absent.
    - ``absent_after``: beets no longer holds the album. Pipeline DB
      clearing fires iff this is True.
    - ``selector_failures``: one entry per selector whose ``sp.run``
      raised or exited non-zero. An empty tuple means every selector
      ran clean. Non-empty does NOT automatically mean the overall
      operation failed — a Discogs-layout album can live under two
      selectors, so failing one while the other removes the album
      still leaves ``absent_after == True``.
    """
    beets_removed: bool
    absent_after: bool
    selector_failures: tuple[SelectorFailure, ...]


def _run_remove_selector(selector: str) -> SelectorFailure | None:
    """Run ``beet remove -d <selector>`` once, never raise.

    Returns ``None`` on clean exit (rc=0), otherwise a
    ``SelectorFailure``. This is the one place that touches the beets
    subprocess; isolating it means the loop in
    ``remove_and_reset_release`` can be trivially correct ("always
    iterate every selector, collect any failures").
    """
    try:
        proc = sp.run(
            ["beet", "remove", "-d", selector],
            capture_output=True, text=True, timeout=30,
            env=beets_subprocess_env(),
        )
    except sp.TimeoutExpired as exc:
        msg = f"timed out after {exc.timeout}s"
        log.warning(
            "release_cleanup: beet remove -d %s %s", selector, msg)
        return SelectorFailure(
            selector=selector, reason="timeout", detail=msg)
    except OSError as exc:
        msg = f"{type(exc).__name__}: {exc}"
        log.warning(
            "release_cleanup: beet remove -d %s raised %s",
            selector, msg)
        return SelectorFailure(
            selector=selector, reason="exception", detail=msg)

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip().splitlines()
        msg = stderr[-1] if stderr else f"rc={proc.returncode}"
        log.warning(
            "release_cleanup: beet remove -d %s exited %d: %s",
            selector, proc.returncode, msg)
        return SelectorFailure(
            selector=selector, reason="nonzero_rc",
            detail=f"rc={proc.returncode}: {msg}")

    return None


def remove_and_reset_release(
    beets_db: "BeetsDB",
    pipeline_db: "PipelineDB",
    release_id: str,
    request_id: int,
) -> ReleaseCleanupResult:
    """Atomically remove a release from beets and clear pipeline ghost state.

    See ``ReleaseCleanupResult`` for the return contract.

    Preconditions: ``release_id`` is non-empty. Callers that may pass
    an empty ID must guard before invoking.
    """
    if not release_id:
        raise ValueError("release_id must be non-empty")

    before = beets_db.locate(release_id)
    album_was_in_beets = before.kind == "exact"

    failures: list[SelectorFailure] = []
    if album_was_in_beets:
        # ``before.selectors`` is every selector the ID could live
        # under (one for UUIDs, two for Discogs numerics). We iterate
        # EVERY selector unconditionally — catching per-selector
        # failures in ``_run_remove_selector`` — so a timeout on one
        # never leaves the others untried. That's the PR #123B bug:
        # the raw loop raised out on the first ``TimeoutExpired``,
        # after the ban-source caller had committed the denylist row.
        # NB: when selector N times out, Python kills the child process
        # before moving on. Beets uses a file-backed SQLite DB, so a
        # killed-mid-transaction remove can leave the WAL in a state
        # where selector N+1 briefly blocks acquiring the write lock.
        # SQLite clears this on its own; tests mock subprocess so they
        # can't exercise it, but if production logs show lock contention
        # the fix is to increase the timeout or serialize retries.
        for selector in before.selectors:
            failure = _run_remove_selector(selector)
            if failure is not None:
                failures.append(failure)

    after = beets_db.locate(release_id)
    absent_after = after.kind != "exact"
    beets_removed = album_was_in_beets and absent_after

    if absent_after:
        pipeline_db.clear_on_disk_quality_fields(request_id)

    return ReleaseCleanupResult(
        beets_removed=beets_removed,
        absent_after=absent_after,
        selector_failures=tuple(failures),
    )
