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
"""

from __future__ import annotations

import subprocess as sp
from typing import TYPE_CHECKING

from lib.util import beets_subprocess_env

if TYPE_CHECKING:
    from lib.beets_db import BeetsDB
    from lib.pipeline_db import PipelineDB


def remove_and_reset_release(
    beets_db: "BeetsDB",
    pipeline_db: "PipelineDB",
    release_id: str,
    request_id: int,
) -> tuple[bool, bool]:
    """Atomically remove a release from beets and clear pipeline ghost state.

    Returns ``(beets_removed, absent_after)``:
    - ``beets_removed``: True iff the album was present before this
      call AND is absent afterward — i.e. THIS call removed it.
      (An out-of-band prior removal returns False here; it still
      clears the pipeline DB.)
    - ``absent_after``: True iff beets no longer holds the album
      after this call. Clearing fires iff this is True.

    Preconditions: ``release_id`` is non-empty. Callers that may pass
    an empty ID must guard before invoking.
    """
    if not release_id:
        raise ValueError("release_id must be non-empty")

    before = beets_db.locate(release_id)
    album_was_in_beets = before.kind == "exact"

    if album_was_in_beets:
        # ``before.selectors`` is every selector the ID could live
        # under (one for UUIDs, two for Discogs numerics). Running
        # all of them makes the remove idempotent across layouts.
        for selector in before.selectors:
            sp.run(
                ["beet", "remove", "-d", selector],
                capture_output=True, text=True, timeout=30,
                env=beets_subprocess_env(),
            )

    after = beets_db.locate(release_id)
    absent_after = after.kind != "exact"
    beets_removed = album_was_in_beets and absent_after

    if absent_after:
        pipeline_db.clear_on_disk_quality_fields(request_id)

    return beets_removed, absent_after
