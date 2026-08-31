"""Write one album's file tags from the Beets DB — the tag-sync lane (#1260).

The import-time MusicBrainz merge retag (``lib/beets_retag.py``) is
deliberately ``-W``: the Beets DB moves to the merge survivor in one atomic
``Album.store()`` transaction and no file is touched. The accepted residual
is an installed album whose file tags still name the merged-away id until a
successful import rewrites them — and for a long-tail ``wanted`` request
that import may never come. The daily census (#1142) surfaces the cohort;
this lane heals it: one ``beet write`` scoped to exactly one album, written
DB→file, verified by re-reading the files themselves.

This is an explicitly authorized Beets mutation lane (issue #1260 — the
operator decision is quoted there), with one canonical execution path and
three callers: the dashboard button's web route, its ``pipeline-cli``
HTTP adapter, and the merge seam itself (best-effort, after the rekey has
durably landed — ``lib/download_validation.py``). It writes file TAGS only:
``beet write`` never moves a file, and this lane never chooses a value —
it propagates what the Beets DB already holds. Honestly scoped: ``beet
write`` diffs and writes EVERY out-of-sync media tag field for the matched
items (``library.Item._media_tag_fields``), not only ``mb_albumid`` — the
DB is authority on all of them after import/retag, and an operator's
out-of-band file-tag edit that never reached the DB is overwritten
(#1260 review F9).

Two measured properties of the pinned beets runtime (#1260 review F4/F5):

* ``beet write`` runs each item through ``item.try_sync(True, False)``,
  which stores the item's DB ``mtime`` alongside the file write — so
  every SUCCESSFULLY written item does NOT arm the ``beet update``
  copy-back hazard the census module documents; the written file reads
  as current, not modified. Pinned against the real subprocess in
  ``tests/test_beets_tag_sync.py``. The write-FAILURE path is the one
  that can still arm it: ``try_write`` catches a save that raised after
  mutagen already touched the file and ``try_sync`` then stores the
  STALE mtime (#1260 re-review C1) — the census re-flags that album, and
  the button retries.
* A tag write normally lands inside existing tag padding: measured across
  flac/opus/mp3 with both a same-length identity swap and eight added
  fields, file SIZE was byte-identical in all six probes — so evidence
  fingerprints (``lib/quality_evidence.py``, size-based) are normally
  unaffected. The residual: a file with no padding headroom forces a
  container rewrite and a size change, which makes every fingerprint
  witness (merge-rekey adoption, HAVE staleness, sidecar backfill, world
  audit) fail CLOSED — an operator-visible refusal, never silent
  corruption. Recorded in ``lib/merge_rekey_service.py``'s witness
  docstring, whose lane enumeration this module extends.

Design, deliberately parallel to ``lib/beets_retag.py``:

* **Compare-and-set, twice.** The service refuses unless the album's DB
  identity equals the identity the caller authorized
  (``expected_mb_albumid``), and the write query itself re-pins BOTH facts
  as exact-match tokens — ``album_id:=<id>`` (the row set the pre-check
  authorized) AND ``mb_albumid:=<identity>`` (the value it authorized) — so
  an album that moved between the read and the subprocess matches nothing.
  Live-verified against a real ``beet write``: a stale identity answers
  ``No matching items found.`` (exit 1) and touches no file. Unlike the
  retag's album-level query, these are ITEM query tokens: ``write`` targets
  items, and after any album-level identity move ``Album.store()`` has
  already fanned the value onto every item row (verified in the same
  spike), so the item-level pin selects exactly the authorized album's
  items.
* **The effect surface equals the verification surface.** The retag's
  guard re-reads the Beets DB because ``-W`` makes the DB the entire
  effect; this lane's effect is file tags, so its verdict comes ONLY from
  re-reading file tags — via the census's own single-album scan
  (``lib.retag_divergence_audit.scan_retag_divergence_single_album``), the
  same classifier and tag reader the dashboard card renders. The write
  subprocess's exit status is never evidence in either direction: a green
  exit with divergent re-read tags is ``residual_divergence``; a raised
  subprocess whose write actually landed is ``synced``.
* **Terminal convergence step, not an authorization link.** Nothing
  downstream consumes this result to decide anything; a partial failure
  leaves some files synced and some stale — strictly closer to
  convergence, re-flagged by the census, retried by the button. This is
  what licenses a file-mutating step the import-time retag itself must not
  take (issue #1260's design discussion).
* **Non-blocking RELEASE lock.** The write runs holding
  ``RELEASE(<identity>)`` so an operator destructive action resolving "the
  one album at this release" never interleaves with it; contention is a
  typed retryable outcome, never a wait (the ``lib/download_validation.py``
  merge-seam precedent).
"""

from __future__ import annotations

import logging
import subprocess as sp
from collections.abc import Callable
from contextlib import AbstractContextManager, closing
from typing import Final, Protocol

import msgspec

from lib.beets_child import BeetsChildRun, SubprocessRunFn, run_pinned_beets_child
from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity, normalize_release_id
from lib.retag_divergence_audit import (
    OwnedSingleAlbumRetagDivergenceBeetsDB,
    RetagDivergenceAlbum,
    SingleAlbumRetagDivergenceBeetsDB,
    _BeetsAuthorityUnavailable,
    _open_beets_authority,
    read_mb_albumid_tag,
    scan_retag_divergence_single_album,
)

log = logging.getLogger("cratedigger")

#: ``beet write`` performs one real tag write per item; albums are small
#: (rarely >20 items) but the library lives on virtiofs, so this bound is
#: generous — it exists only so a wedged subprocess cannot stall a caller.
TAG_SYNC_TIMEOUT_SECONDS: Final = 300

#: The write ran and the re-read file tags now agree with the DB.
RESULT_SYNCED: Final = "synced"
#: Every readable file already agreed (or the album has no items); no
#: write was attempted.
RESULT_ALREADY_SYNCED: Final = "already_synced"
#: No Beets album with this id (album entry), or no album at this release
#: id (release entry).
RESULT_NOT_FOUND: Final = "not_found"
#: The release entry resolved more than one current album — merging or
#: deleting either is the operator's call; nothing was touched.
RESULT_NOT_UNIQUE: Final = "not_unique"
#: The caller's authorized identity is not what the DB names (or is not a
#: MusicBrainz release id at all); nothing was touched.
RESULT_IDENTITY_MISMATCH: Final = "identity_mismatch"
#: The album row carries no ``mb_albumid`` — there is no DB identity to
#: write; this lane never syncs an absence over a populated file tag.
RESULT_DB_IDENTITY_ABSENT: Final = "db_identity_absent"
#: Another process holds the RELEASE lock on this identity. Nothing was
#: attempted; retry re-derives.
RESULT_RELEASE_LOCKED: Final = "release_locked"
#: The write ran but the re-read file tags still disagree (an unreadable
#: file, a write the subprocess refused, or a genuine failure) — surfaced
#: with the per-item detail; the census keeps flagging it.
RESULT_RESIDUAL_DIVERGENCE: Final = "residual_divergence"
#: The Beets authority could not be opened or read.
RESULT_BEETS_UNAVAILABLE: Final = "beets_unavailable"

#: The route's status mapping (CLI ⇄ API convention table: 200/0 success,
#: 404/2 not found, 409/4 wrong state, 503/5 transient/retryable). The CLI
#: adapter relays the route, so these statuses ARE its exit codes via
#: ``scripts/pipeline_cli/api_mutations.py::_exit_code``'s default mapping.
TAG_SYNC_HTTP_STATUS: Final[dict[str, int]] = {
    RESULT_SYNCED: 200,
    RESULT_ALREADY_SYNCED: 200,
    RESULT_NOT_FOUND: 404,
    RESULT_NOT_UNIQUE: 409,
    RESULT_IDENTITY_MISMATCH: 409,
    RESULT_DB_IDENTITY_ABSENT: 409,
    RESULT_RESIDUAL_DIVERGENCE: 409,
    RESULT_RELEASE_LOCKED: 503,
    RESULT_BEETS_UNAVAILABLE: 503,
}

#: The album pre/post classes that mean "nothing left to write": every
#: readable file agrees, or there are no files at all.
_CONVERGED_ALBUM_CLASSES: Final = frozenset({"agrees", "empty"})


#: The injected ``beet write`` runner — ``query_tokens`` is
#: :func:`tag_sync_query`'s compound item query, as SEPARATE argv elements.
#: A definition-time default on the sync entry points; tests inject a
#: replacement and never patch the module binding. The run record is
#: diagnostic detail only: the re-read file tags are the only evidence of
#: the end state (the ``lib/beets_retag.py`` doctrine, at the file layer).
type TagSyncWriteFn = Callable[[tuple[str, str]], BeetsChildRun]


class TagSyncResult(msgspec.Struct, frozen=True):
    """The one typed answer this lane gives.

    ``album`` carries the single-album census scan that decided the
    verdict (post-write for ``synced``/``residual_divergence``, pre-write
    for ``already_synced``/``release_locked``) so the dashboard can
    re-render the row from the same classification the census itself
    would produce.
    """

    outcome: str
    album_id: int | None = None
    db_mb_albumid: str | None = None
    album: RetagDivergenceAlbum | None = None
    error_message: str | None = None


class TagSyncLockDB(Protocol):
    """The one PipelineDB surface this lane uses: the RELEASE lock."""

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...


#: The Beets read surface is EXACTLY the census's single-album protocol —
#: this lane verifies through the same instrument the dashboard renders.
TagSyncBeetsDB = SingleAlbumRetagDivergenceBeetsDB
OwnedTagSyncBeetsDB = OwnedSingleAlbumRetagDivergenceBeetsDB


class ReleaseTagSyncBeetsDB(SingleAlbumRetagDivergenceBeetsDB, Protocol):
    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...


class OwnedReleaseTagSyncBeetsDB(ReleaseTagSyncBeetsDB, Protocol):
    def close(self) -> None: ...


type TagSyncBeetsFactory = Callable[[], OwnedTagSyncBeetsDB]
type ReleaseTagSyncBeetsFactory = Callable[[], OwnedReleaseTagSyncBeetsDB]


def tag_sync_query(
    identity: ReleaseIdentity, *, album_id: int,
) -> tuple[str, str]:
    """Two ANDed exact-match ITEM query tokens naming precisely the one
    album's items — and only while their DB rows still carry the
    authorized identity.

    ``album_id:=<id>`` pins the items of the exact album row the
    pre-check authorized (``items.album_id`` — an integer column; the
    ``=``-prefix ``MatchQuery`` compares by SQL equality, and SQLite's
    INTEGER affinity makes the text operand exact). ``mb_albumid:=<id>``
    is the identity compare-and-set: a row whose value moved since the
    pre-check's read matches nothing. Both are QUERY tokens to
    ``modify_parse_args``-style classification (each ``:`` precedes its
    first ``=``), passed as SEPARATE argv elements because beets ANDs
    distinct tokens implicitly — the exact ``lib/beets_retag.py::
    retag_album_query`` discipline, one selection mechanism shared with
    the guard that verifies the outcome.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "tag-sync query is MusicBrainz-only; refusing to build a query "
            f"for {identity.source} release {identity.release_id}"
        )
    return (f"album_id:={album_id}", f"mb_albumid:={identity.release_id}")


def run_beets_write_tags(
    query_tokens: tuple[str, str],
    *,
    runner: SubprocessRunFn = sp.run,
) -> BeetsChildRun:
    """Run ``beet write`` for the compound item query in the
    deployment-supplied Beets runtime.

    Invoked as ``<beets python> -m beets write <album_id-token>
    <mb_albumid-token>`` — no flags: ``write`` has no interactive prompt,
    never moves files, and takes no ``-y``. The spawn goes through
    ``lib/beets_child.py::run_pinned_beets_child``, which resolves the
    interpreter and environment from ``lib/util.py::beets_subprocess_env``
    — the single source of truth for how a beets subprocess finds its
    config.

    Raises on a launch/timeout failure; the caller converts that into a
    diagnostic note and lets the re-read files decide the outcome.
    """
    proc = run_pinned_beets_child(
        ["-m", "beets", "write", *query_tokens],
        timeout=TAG_SYNC_TIMEOUT_SECONDS,
        runner=runner,
    )
    return BeetsChildRun.from_completed(proc)


def _refusal(
    outcome: str,
    *,
    album_id: int | None,
    db_mb_albumid: str | None = None,
    album: RetagDivergenceAlbum | None = None,
    message: str,
) -> TagSyncResult:
    return TagSyncResult(
        outcome=outcome,
        album_id=album_id,
        db_mb_albumid=db_mb_albumid,
        album=album,
        error_message=message,
    )


def sync_album_file_tags(
    beets: TagSyncBeetsDB,
    lock_db: TagSyncLockDB,
    *,
    album_id: int,
    expected_mb_albumid: str,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    run_write: TagSyncWriteFn = run_beets_write_tags,
) -> TagSyncResult:
    """Sync exactly one album's file tags from its Beets DB identity.

    Pure composition over an already-open ``beets`` handle — no
    availability mediation here; the ``*_from_factory`` entry points own
    that, mirroring ``lib/retag_divergence_audit.py``'s split. Beets-read
    exceptions propagate to those wrappers; ``run_write`` exceptions are
    converted to a diagnostic note and never decide anything — the
    post-write re-read of the files does.
    """
    from lib.pipeline_db import (
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key,
    )

    expected = normalize_release_id(expected_mb_albumid)
    identity = ReleaseIdentity.from_id(expected) if expected else None
    if identity is None or identity.source != "musicbrainz":
        return _refusal(
            RESULT_IDENTITY_MISMATCH,
            album_id=album_id,
            message=(
                f"{expected_mb_albumid!r} is not a MusicBrainz release id; "
                "this lane only writes MusicBrainz identities"
            ),
        )

    row = beets.get_album_mb_identity(album_id)
    if row is None:
        return _refusal(
            RESULT_NOT_FOUND,
            album_id=album_id,
            message=f"no Beets album with id {album_id}",
        )
    db_identity = row.mb_albumid
    if not db_identity:
        return _refusal(
            RESULT_DB_IDENTITY_ABSENT,
            album_id=album_id,
            db_mb_albumid="",
            message=(
                f"Beets album {album_id} has no mb_albumid; there is no DB "
                "identity to write to its files"
            ),
        )
    if db_identity != identity.release_id:
        return _refusal(
            RESULT_IDENTITY_MISMATCH,
            album_id=album_id,
            db_mb_albumid=db_identity,
            message=(
                f"Beets album {album_id} now names {db_identity}, not the "
                f"authorized {identity.release_id}; recheck and retry with "
                "the current identity"
            ),
        )

    pre = scan_retag_divergence_single_album(
        beets, album_id, read_tag=read_tag,
    )
    if pre is None:
        return _refusal(
            RESULT_NOT_FOUND,
            album_id=album_id,
            db_mb_albumid=db_identity,
            message=f"Beets album {album_id} disappeared during the sync",
        )
    if pre.album_class in _CONVERGED_ALBUM_CLASSES:
        return TagSyncResult(
            outcome=RESULT_ALREADY_SYNCED,
            album_id=album_id,
            db_mb_albumid=db_identity,
            album=pre,
        )
    if not any(item.item_class == "diverges" for item in pre.items):
        # Every non-agreeing item is unreadable (or a refused path) — a
        # write cannot heal what cannot be read back, so launching the
        # subprocess would only re-fail forever (#1260 review F6). The
        # card's button is gated the same way client-side; this is the
        # seam's own gate. Files untouched. Known residual (#1260
        # re-review C3): a MIXED album — a readable diverging item beside
        # a permanently unreadable one — passes this gate, heals the
        # readable siblings on the first pass, and then reports
        # ``residual_divergence`` on every later trigger (the re-run
        # write is a no-op; wasted subprocess work, never repeated
        # mutation), because ``unreadable`` outranks ``diverges`` in the
        # album display class.
        return _refusal(
            RESULT_RESIDUAL_DIVERGENCE,
            album_id=album_id,
            db_mb_albumid=db_identity,
            album=pre,
            message=(
                "no readable file tag disagrees; the non-agreeing items "
                "are unreadable or refused, and a write cannot heal them"
            ),
        )

    # See docs/advisory-locks.md. RELEASE on the one identity this write
    # propagates; non-blocking, held across write+verify. Callers under
    # the importer already hold IMPORT outer (the merge seam's own
    # RELEASE pair is released before this runs), preserving the
    # documented IMPORT → RELEASE order; the web route acquires it on the
    # server's thread-local session with no IMPORT held.
    with lock_db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_RELEASE,
        release_id_to_lock_key(identity.release_id),
    ) as acquired:
        if not acquired:
            return _refusal(
                RESULT_RELEASE_LOCKED,
                album_id=album_id,
                db_mb_albumid=db_identity,
                album=pre,
                message=(
                    "another process holds the release lock for "
                    f"{identity.release_id}; nothing was written"
                ),
            )
        try:
            run = run_write(tag_sync_query(identity, album_id=album_id))
        except Exception as exc:  # noqa: BLE001 - external edge, note only
            write_note = f"beet write raised {type(exc).__name__}: {exc}"
            log.warning(
                "beet write for album %s raised: %s", album_id, exc,
            )
        else:
            write_note = f"beet write exited {run.returncode}"
            if run.returncode != 0:
                log.warning(
                    "beet write for album %s exited %s: %s",
                    album_id, run.returncode, run.stderr.strip()[-500:],
                )

        # The exit status decided nothing; the re-read files do.
        post = scan_retag_divergence_single_album(
            beets, album_id, read_tag=read_tag,
        )
    if post is None:
        return _refusal(
            RESULT_NOT_FOUND,
            album_id=album_id,
            db_mb_albumid=db_identity,
            message=(
                f"Beets album {album_id} disappeared during the sync "
                f"({write_note})"
            ),
        )
    if post.album_class in _CONVERGED_ALBUM_CLASSES:
        return TagSyncResult(
            outcome=RESULT_SYNCED,
            album_id=album_id,
            db_mb_albumid=db_identity,
            album=post,
        )
    return _refusal(
        RESULT_RESIDUAL_DIVERGENCE,
        album_id=album_id,
        db_mb_albumid=db_identity,
        album=post,
        message=(
            f"{write_note}, but the re-read file tags still disagree "
            f"(album class {post.album_class}); the census will keep "
            "flagging this album"
        ),
    )


def _sync_with_mediated_beets(
    beets: TagSyncBeetsDB,
    lock_db: TagSyncLockDB,
    *,
    album_id: int,
    expected_mb_albumid: str,
    read_tag: Callable[[str], str],
    run_write: TagSyncWriteFn,
) -> TagSyncResult | _BeetsAuthorityUnavailable:
    from lib.beets_db import beets_authority_availability_category

    try:
        return sync_album_file_tags(
            beets, lock_db,
            album_id=album_id,
            expected_mb_albumid=expected_mb_albumid,
            read_tag=read_tag,
            run_write=run_write,
        )
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return _BeetsAuthorityUnavailable(category)


def _unavailable_result(
    category: str, *, album_id: int | None,
) -> TagSyncResult:
    return TagSyncResult(
        outcome=RESULT_BEETS_UNAVAILABLE,
        album_id=album_id,
        error_message=f"current Beets authority unavailable ({category})",
    )


def sync_album_file_tags_from_factory(
    beets_factory: TagSyncBeetsFactory,
    lock_db: TagSyncLockDB,
    *,
    album_id: int,
    expected_mb_albumid: str,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    run_write: TagSyncWriteFn = run_beets_write_tags,
) -> TagSyncResult:
    """Own Beets open/sync/close; type only expected unavailability."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_result(opened.category, album_id=album_id)
    with closing(opened):
        result = _sync_with_mediated_beets(
            opened, lock_db,
            album_id=album_id,
            expected_mb_albumid=expected_mb_albumid,
            read_tag=read_tag,
            run_write=run_write,
        )
    if isinstance(result, TagSyncResult):
        return result
    return _unavailable_result(result.category, album_id=album_id)


def sync_album_file_tags_from_borrowed_factory(
    beets_factory: Callable[[], TagSyncBeetsDB],
    lock_db: TagSyncLockDB,
    *,
    album_id: int,
    expected_mb_albumid: str,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    run_write: TagSyncWriteFn = run_beets_write_tags,
) -> TagSyncResult:
    """Mediate a server-owned Beets handle without closing its lifecycle."""
    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_result(opened.category, album_id=album_id)
    result = _sync_with_mediated_beets(
        opened, lock_db,
        album_id=album_id,
        expected_mb_albumid=expected_mb_albumid,
        read_tag=read_tag,
        run_write=run_write,
    )
    if isinstance(result, TagSyncResult):
        return result
    return _unavailable_result(result.category, album_id=album_id)


def sync_release_file_tags_from_factory(
    beets_factory: ReleaseTagSyncBeetsFactory,
    lock_db: TagSyncLockDB,
    *,
    release_id: str,
    read_tag: Callable[[str], str] = read_mb_albumid_tag,
    run_write: TagSyncWriteFn = run_beets_write_tags,
) -> TagSyncResult:
    """The merge seam's entry: resolve the ONE album currently at
    ``release_id`` and sync its file tags. Refuses (typed, world
    untouched) unless Beets resolves exactly one current album there —
    merging or deleting an ambiguous pair is the operator's call.
    """
    from lib.beets_db import beets_authority_availability_category

    normalized = normalize_release_id(release_id)
    identity = ReleaseIdentity.from_id(normalized) if normalized else None
    if identity is None or identity.source != "musicbrainz":
        return _refusal(
            RESULT_IDENTITY_MISMATCH,
            album_id=None,
            message=(
                f"{release_id!r} is not a MusicBrainz release id; this lane "
                "only writes MusicBrainz identities"
            ),
        )

    opened = _open_beets_authority(beets_factory)
    if isinstance(opened, _BeetsAuthorityUnavailable):
        return _unavailable_result(opened.category, album_id=None)
    with closing(opened):
        try:
            resolution = opened.resolve_current_release(identity)
        except Exception as exc:
            category = beets_authority_availability_category(exc)
            if category is None:
                raise
            return _unavailable_result(category, album_id=None)
        if isinstance(resolution, CurrentBeetsMissing):
            return _refusal(
                RESULT_NOT_FOUND,
                album_id=None,
                message=(
                    f"Beets holds no current album at {identity.release_id}; "
                    "nothing to sync"
                ),
            )
        if isinstance(resolution, CurrentBeetsAmbiguous):
            return _refusal(
                RESULT_NOT_UNIQUE,
                album_id=None,
                message=(
                    f"Beets cannot name one current album at "
                    f"{identity.release_id} ({resolution.reason}: albums "
                    + ", ".join(str(a) for a in resolution.album_ids)
                    + "); merging or deleting either is an operator decision"
                ),
            )
        assert isinstance(resolution, CurrentBeetsUnique)
        result = _sync_with_mediated_beets(
            opened, lock_db,
            album_id=resolution.album_id,
            expected_mb_albumid=identity.release_id,
            read_tag=read_tag,
            run_write=run_write,
        )
    if isinstance(result, TagSyncResult):
        return result
    return _unavailable_result(result.category, album_id=resolution.album_id)


__all__ = [
    "RESULT_ALREADY_SYNCED",
    "RESULT_BEETS_UNAVAILABLE",
    "RESULT_DB_IDENTITY_ABSENT",
    "RESULT_IDENTITY_MISMATCH",
    "RESULT_NOT_FOUND",
    "RESULT_NOT_UNIQUE",
    "RESULT_RELEASE_LOCKED",
    "RESULT_RESIDUAL_DIVERGENCE",
    "RESULT_SYNCED",
    "TAG_SYNC_HTTP_STATUS",
    "TAG_SYNC_TIMEOUT_SECONDS",
    "OwnedReleaseTagSyncBeetsDB",
    "OwnedTagSyncBeetsDB",
    "ReleaseTagSyncBeetsDB",
    "ReleaseTagSyncBeetsFactory",
    "TagSyncBeetsDB",
    "TagSyncBeetsFactory",
    "TagSyncLockDB",
    "TagSyncResult",
    "TagSyncWriteFn",
    "run_beets_write_tags",
    "sync_album_file_tags",
    "sync_album_file_tags_from_borrowed_factory",
    "sync_album_file_tags_from_factory",
    "sync_release_file_tags_from_factory",
    "tag_sync_query",
]
