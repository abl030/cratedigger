"""Atomic 'remove from beets + reset pipeline state' primitive (issue #121).

Before this module, the ban-source route hand-rolled the coupling:
choose the right selector(s) based on ID shape, run ``beet remove -d``,
verify the album is absent, then call ``clear_on_disk_quality_fields``.
Every Codex round on PR #119 surfaced one of those steps being skipped
— a selector forgotten, a clear missed when the first-pass query
missed a legacy Discogs album, a cleared-but-not-removed race in the
'already gone' path.

The module exposes two entry points and the layering between them is
the point — do not collapse or route around it:

- ``remove_album_by_selectors(beets_db, release_id)`` is the pure
  beets-only primitive. Given a ``release_id`` it locates the album,
  iterates every selector, collects per-selector failures, and
  re-queries to confirm absence. No pipeline DB coupling. Callable
  from the harness (which runs as a subprocess with no PipelineDB
  handle) — this is what the pre-flight same-MBID removal in
  ``harness/import_one.py`` uses to avoid the cross-MBID blast
  radius of beets' ``task.should_remove_duplicates = True`` path.

- ``remove_and_reset_release(beets_db, pipeline_db, release_id,
  request_id)`` wraps the primitive and adds one extra step:
  clearing ``current_spectral_*`` / ``imported_path`` /
  ``verified_lossless`` via
  ``PipelineDB.clear_on_disk_quality_fields`` iff the album is
  absent afterwards. That's what the ban-source web route and other
  pipeline-aware callers need.

Issue #123 PR B: each ``beet remove`` invocation is wrapped in a
try/except that catches ``TimeoutExpired``, non-zero exit codes, and
any ``OSError`` (e.g. ``beet`` missing from PATH). The loop always
attempts every selector, and per-selector failures are surfaced via
``ReleaseCleanupResult.selector_failures`` so the caller can tell
partial failure from a clean run. Before this change, a
``TimeoutExpired`` on selector 1 escaped the loop and left selector 2
untried — *after* the ban-source caller had already committed the
denylist row, leaving the banned copy on disk with no recovery path.

Issue #133: the subprocess primitive now lives in
``lib.beets_album_op`` (``remove_by_selector`` / ``remove_album``)
as part of the ``BeetsAlbumOp`` extraction. ``SelectorFailure`` is a
kept-name alias for ``BeetsOpFailure``; ``SelectorFailureReason`` is
the kept-name alias for ``BeetsOpFailureReason``. Existing callers
(web routes, tests) continue to import from this module unchanged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib.beets_album_op import (BeetsOpFailure as SelectorFailure,
                                BeetsOpFailureReason as SelectorFailureReason,
                                remove_album as _remove_album_op,
                                remove_by_selector)
from lib.beets_album_op import BeetsAlbumHandle

if TYPE_CHECKING:
    from lib.beets_db import BeetsDB
    from lib.pipeline_db import PipelineDB


log = logging.getLogger("cratedigger")

# Re-export for historical call sites (web/routes/pipeline.py, tests,
# harness/import_one.py) that import these names from this module.
__all__ = [
    "SelectorFailure",
    "SelectorFailureReason",
    "ReleaseCleanupResult",
    "remove_album_by_beets_id",
    "remove_album_by_selectors",
    "remove_and_reset_release",
]


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


def remove_album_by_beets_id(album_id: int) -> SelectorFailure | None:
    """Remove a single album by its beets numeric primary key.

    Narrower than ``remove_album_by_selectors``: the ``id:<N>``
    selector is a ``SELECT ... WHERE id = ?`` — a beets numeric PK
    is unique by construction, so this cannot match any album but
    the one. Used by the harness to remove a stale same-MBID entry
    AFTER a successful upgrade import: during the import both the
    old and new albums briefly coexist (new is imported to a
    disambiguated path via ``%aunique``), so we can't scope the
    removal by MBID. Matching by beets id is the only selector
    narrow enough to be safe — ``mb_albumid:<uuid>`` would match
    both.

    Issue #133: now a thin adapter over ``beets_album_op.remove_album``
    — returns the underlying ``BeetsOpFailure`` (aliased as
    ``SelectorFailure`` for historical callers) or ``None`` on clean
    exit. Argv construction is centralised in ``lib.beets_album_op``.
    """
    result = _remove_album_op(BeetsAlbumHandle(album_id=album_id))
    return result.failure


def remove_album_by_selectors(
    beets_db: "BeetsDB",
    release_id: str,
) -> ReleaseCleanupResult:
    """Remove a release from beets via its canonical selectors.

    Pure beets-only primitive — does NOT touch the pipeline DB. See
    ``ReleaseCleanupResult`` for the return contract.

    Called from:
    - ``remove_and_reset_release`` (this module) which wraps this and
      adds pipeline-side ``clear_on_disk_quality_fields``
    - ``harness/import_one.py`` pre-flight, where a stale same-MBID
      entry must be removed before the beets interactive import
      starts — without touching cross-MBID sibling pressings that
      beets' own ``remove_duplicates()`` would have destroyed

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
        # under (one for UUIDs, two for Discogs numerics). Iterate
        # EVERY selector unconditionally — ``remove_by_selector``
        # catches per-selector failures so a timeout on one never
        # leaves the others untried. That's the PR #123B bug: the raw
        # loop raised out on the first ``TimeoutExpired``, after the
        # ban-source caller had committed the denylist row.
        # NB: when selector N times out, Python kills the child process
        # before moving on. Beets uses a file-backed SQLite DB, so a
        # killed-mid-transaction remove can leave the WAL in a state
        # where selector N+1 briefly blocks acquiring the write lock.
        # SQLite clears this on its own; tests mock subprocess so they
        # can't exercise it, but if production logs show lock contention
        # the fix is to increase the timeout or serialize retries.
        for selector in before.selectors:
            failure = remove_by_selector(selector)
            if failure is not None:
                failures.append(failure)

    after = beets_db.locate(release_id)
    absent_after = after.kind != "exact"
    beets_removed = album_was_in_beets and absent_after

    return ReleaseCleanupResult(
        beets_removed=beets_removed,
        absent_after=absent_after,
        selector_failures=tuple(failures),
    )


def remove_and_reset_release(
    beets_db: "BeetsDB",
    pipeline_db: "PipelineDB",
    release_id: str,
    request_id: int,
) -> ReleaseCleanupResult:
    """Atomically remove a release from beets and clear pipeline ghost state.

    Thin wrapper around ``remove_album_by_selectors`` that adds the
    pipeline-DB cleanup: iff the album is absent afterwards, clear
    stale ``current_spectral_*`` / ``imported_path`` /
    ``verified_lossless`` so downstream consumers don't reason about
    ghost state left behind by the removed album.

    See ``ReleaseCleanupResult`` for the return contract.

    Preconditions: ``release_id`` is non-empty. Callers that may pass
    an empty ID must guard before invoking.
    """
    if not release_id:
        raise ValueError("release_id must be non-empty")

    result = remove_album_by_selectors(beets_db, release_id)

    if result.absent_after:
        pipeline_db.clear_on_disk_quality_fields(request_id)

    return result
