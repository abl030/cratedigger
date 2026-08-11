"""Follow a MusicBrainz merge on the Beets side, at exactly one album (#1059).

When MusicBrainz merges release A into release B, the installed album still
carries A in ``mb_albumid``. The request cannot simply be rekeyed to B first:
Beets keys album duplicate detection on ``mb_albumid``
(``duplicate_keys: album: [mb_albumid, discogs_albumid]``, combined as an
``AndQuery`` in ``beets/library/models.py::duplicates_query``), so an incoming
album at B would not match a library album filed at A. No duplicate would be
flagged, the import would land a SECOND album, and the existing-album lookup
would miss — routing the quality decision through ``import_no_exist`` and
silently skipping the downgrade guard on exactly the albums we already hold.

So the library moves first. This module runs Beets' own ``mbsync`` — the
command for "MusicBrainz changed its mind" — under an anchored query on the
old ID, and then re-reads the library to decide what actually happened. Only
when the observable end state is "the old ID is gone and the new ID is uniquely
held" may the caller rekey the request.

Four properties are load-bearing:

* **The query names one album.** ``mbsync`` accepts a query and will happily
  retag everything it matches. :func:`mbsync_album_query` anchors the regex so
  it can only ever name albums filed under exactly the old ID. Note that
  ``beetsplug/mbsync.py::func`` runs ``self.singletons(lib, [*query,
  "singleton:true"], …)`` BEFORE ``self.albums(query)``: the command is two
  passes, not one. The singleton pass keys on ``mb_trackid`` and cannot move an
  ``mb_albumid``, and an album's items are not singletons, so the anchored
  album query still reaches exactly one album — but the invariant is "this
  query can only name our album", not "mbsync only looks at albums".
* **Identity only — the files never move.** ``mbsync`` is a metadata command
  that ALSO relocates files: ``func`` computes ``move =
  ui.should_move(opts.move)``, which defaults to ``config['import']['move'] or
  config['import']['copy']``, and ``lib/beets_config_contract.py`` hard-requires
  ``import.move: yes``. With that default it calls ``item.move()`` and
  ``album.move()``, so a merge that changes any path component (`$albumartist`,
  `$year`, `$album`, or ``path_disambig`` — which is exactly the field family
  that made two entries look like duplicates) would rename the album directory:
  new Jellyfin item identities (identity is MD5 of the path) dropping the album
  into "Recently Added", the documented Plex album-split footgun, and
  ``Item.move()``'s vacated-directory prune deleting the ``cratedigger.json``
  verified-lossless sidecar as ``clutter``. :func:`run_beets_mbsync` therefore
  passes ``-M`` / ``--nomove``. We are following an identity change, not
  reorganising the library.
* **``mbsync``'s exit status is not evidence.** It logs-and-skips a release it
  cannot fetch and still exits 0. Nothing about the subprocess decides the
  outcome; the re-read library does.
* **An unreadable Beets authority is never absence.** A resolver that omits an
  identity, or raises, is a failure — never "the album is not held". Reading it
  as absence would authorize a rekey that manufactures a duplicate pressing.

Nothing here is durable. A failure leaves the library and the request exactly
as they were, and the next sweep re-derives the same world (invariant 11 —
broken worlds surface and restart; nothing is parked).
"""

from __future__ import annotations

import logging
import re
import subprocess as sp
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, Literal, Protocol

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity

log = logging.getLogger("cratedigger")

#: ``mbsync`` fetches one release from the configured MusicBrainz endpoint,
#: rewrites tags on every track, and stores the album row. The local mirror
#: answers in milliseconds; the bound exists so a wedged endpoint cannot stall
#: a whole sweep.
MBSYNC_TIMEOUT_SECONDS = 120

#: ``-M`` is ``--nomove``: ``beetsplug/mbsync.py`` declares it as
#: ``action="store_false", dest="move"``, and ``beets/ui/__init__.py::
#: should_move`` returns that explicit ``False`` instead of falling back to
#: ``import.move or import.copy`` (which our config contract pins to True).
#: Without it, ``apply_item_changes`` calls ``item.move()`` and ``mbsync``
#: calls ``album.move()`` — see the module docstring for what that destroys.
#: This flag is load-bearing, not a tidiness preference.
MBSYNC_NOMOVE_FLAG: Final = "-M"

#: The library observably moved from the old ID to the new one.
RETAG_RETAGGED: Final = "retagged"
#: The library was already filed under the new ID before we did anything.
RETAG_ALREADY_CURRENT: Final = "already_current"
#: The library holds neither ID — there is nothing to retag.
RETAG_NOT_HELD: Final = "not_held"
#: The library cannot authorize one album for one of the two IDs, or holds
#: both. Fails closed: merging or deleting an album is the operator's call.
RETAG_AMBIGUOUS: Final = "ambiguous"
#: The retag was attempted or could not be attempted, and the library is not
#: at the new ID. Nothing was rekeyed.
RETAG_FAILED: Final = "failed"

type RetagOutcome = Literal[
    "retagged", "already_current", "not_held", "ambiguous", "failed",
]

#: The outcomes that mean "the library is at the current ID (or holds neither),
#: so the caller may rekey the request". Every other outcome leaves the request
#: exactly as it was.
RETAG_READY_OUTCOMES: Final = frozenset({
    RETAG_RETAGGED,
    RETAG_ALREADY_CURRENT,
    RETAG_NOT_HELD,
})


@dataclass(frozen=True)
class MbsyncRun:
    """What one ``beets mbsync`` invocation reported.

    Kept for the diagnostic detail only. ``returncode`` is deliberately NOT a
    decision input: ``mbsync`` logs and skips a release it cannot fetch and
    still exits 0.
    """

    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class BeetsRetagResult:
    """The one typed answer this module gives, plus operator-facing detail."""

    outcome: RetagOutcome
    detail: str


#: The injected ``mbsync`` runner. It is a definition-time default on
#: :func:`retag_merged_album`, so tests pass a replacement explicitly and
#: never patch the module binding — patching does not replace a captured
#: default (`.claude/rules/code-quality.md` § mocks, strategy 2).
type MbsyncFn = Callable[[str], MbsyncRun]

type SubprocessRunFn = Callable[..., sp.CompletedProcess[bytes]]


class CurrentReleaseResolver(Protocol):
    """The only Beets read this module needs.

    Declared locally on purpose: the retag boundary depends on one method, not
    on the whole ``BeetsDB`` surface, and there is no shared request-identity
    module to hang it off. ``lib.beets_db.BeetsDB`` satisfies it structurally.
    """

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        ...


def mbsync_album_query(identity: ReleaseIdentity) -> str:
    """The anchored Beets query naming exactly the album filed under ``identity``.

    ``mbsync`` retags everything the query matches, so the regex is anchored:
    ``mb_albumid::^<escaped-id>$`` cannot match a longer id that merely
    contains this one. An unanchored or substring query is the difference
    between retagging one album and retagging part of the library.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "mbsync retag is MusicBrainz-only; refusing to build a query for "
            f"{identity.source} release {identity.release_id}"
        )
    return f"mb_albumid::^{re.escape(identity.release_id)}$"


def run_beets_mbsync(
    query: str,
    *,
    runner: SubprocessRunFn = sp.run,
    timeout: int = MBSYNC_TIMEOUT_SECONDS,
) -> MbsyncRun:
    """Run ``mbsync -M`` for one query in the deployment-supplied Beets runtime.

    Invoked as ``<beets python> -m beets mbsync -M <query>``: ``python -m
    beets`` is a valid entry point in the pinned 2.13.1, and depending on a
    ``beet`` binary being on this process's PATH would silently pick up
    whatever beets the invoking user happens to have. The interpreter and
    environment come from ``lib/util.py::beets_subprocess_env`` — the single
    source of truth for how a beets subprocess finds its config and
    interpreter.

    ``-M`` (:data:`MBSYNC_NOMOVE_FLAG`) is not optional: without it this
    command renames and relocates the album whenever the merge changes a path
    component, and prunes the vacated directory's ``clutter`` — including the
    ``cratedigger.json`` verified-lossless sidecar. We follow an identity
    change; the files stay exactly where they are.

    Raises on a launch/timeout failure; :func:`retag_merged_album` turns that
    into a typed outcome after re-reading the library.
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env.get("CRATEDIGGER_BEETS_PYTHON", "")
    if not python:
        raise RuntimeError("CRATEDIGGER_BEETS_PYTHON is not configured")
    proc = runner(
        [python, "-m", "beets", "mbsync", MBSYNC_NOMOVE_FLAG, query],
        capture_output=True,
        timeout=timeout,
        env=env,
    )
    return MbsyncRun(
        returncode=proc.returncode,
        stdout=proc.stdout.decode("utf-8", errors="replace"),
        stderr=proc.stderr.decode("utf-8", errors="replace"),
    )


def _describe(resolution: CurrentBeetsResolution) -> str:
    """One short operator-facing phrase for one resolver answer."""
    if isinstance(resolution, CurrentBeetsUnique):
        return f"uniquely held as album {resolution.album_id}"
    if isinstance(resolution, CurrentBeetsMissing):
        return "not held"
    return (
        f"ambiguous ({resolution.reason}) across albums "
        + ", ".join(str(album_id) for album_id in resolution.album_ids)
    )


def _failed(detail: str) -> BeetsRetagResult:
    return BeetsRetagResult(outcome=RETAG_FAILED, detail=detail)


def _resolve_pair(
    beets: CurrentReleaseResolver,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
) -> tuple[CurrentBeetsResolution, CurrentBeetsResolution] | str:
    """Resolve both identities in one snapshot, or describe the failure.

    Returning the pair or an error string keeps every authority failure a
    typed outcome rather than an exception escaping into a sweep over
    thousands of rows. An omitted identity is an authority failure too: the
    resolver's contract is one answer per requested identity, and reading a
    missing key as "not held" would authorize a rekey on no evidence.
    """
    try:
        resolutions = beets.resolve_current_releases(
            [old_identity, new_identity],
        )
    except Exception as exc:  # noqa: BLE001 - authority read, typed outcome
        return (
            "Beets current-release authority could not be read: "
            f"{type(exc).__name__}: {exc}"
        )
    old = resolutions.get(old_identity)
    new = resolutions.get(new_identity)
    if old is None or new is None:
        missing = [
            identity.release_id
            for identity, resolution in (
                (old_identity, old), (new_identity, new),
            )
            if resolution is None
        ]
        return (
            "Beets current-release authority returned no answer for: "
            + ", ".join(missing)
        )
    return (old, new)


def retag_merged_album(
    beets: CurrentReleaseResolver,
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
    run_mbsync: MbsyncFn = run_beets_mbsync,
) -> BeetsRetagResult:
    """Move the one installed album from ``old_identity`` to ``new_identity``.

    Returns an outcome in :data:`RETAG_READY_OUTCOMES` only when the library
    is observably at the new ID or holds neither — that, and nothing else,
    authorizes the caller to rekey the request.

    ``run_mbsync`` is a definition-time default: tests INJECT a replacement,
    they never patch the module binding, because patching does not replace a
    captured default (`.claude/rules/code-quality.md` § mocks, strategy 2).
    """
    if old_identity == new_identity:
        return _failed(
            "refusing to retag a release onto itself: "
            f"{old_identity.release_id}"
        )
    if old_identity.source != "musicbrainz" or new_identity.source != "musicbrainz":
        return _failed(
            "mbsync retag is MusicBrainz-only; refusing "
            f"{old_identity.source} {old_identity.release_id} -> "
            f"{new_identity.source} {new_identity.release_id}"
        )

    resolved = _resolve_pair(beets, old_identity, new_identity)
    if isinstance(resolved, str):
        return _failed(resolved)
    old, new = resolved

    # Fail closed on either side: an exact ID whose cardinality or topology
    # cannot name one current album cannot be retagged onto, or away from.
    if isinstance(old, CurrentBeetsAmbiguous) or isinstance(new, CurrentBeetsAmbiguous):
        return BeetsRetagResult(
            outcome=RETAG_AMBIGUOUS,
            detail=(
                f"{old_identity.release_id} is {_describe(old)}; "
                f"{new_identity.release_id} is {_describe(new)}"
            ),
        )

    if isinstance(old, CurrentBeetsMissing):
        if isinstance(new, CurrentBeetsUnique):
            return BeetsRetagResult(
                outcome=RETAG_ALREADY_CURRENT,
                detail=(
                    f"library already holds {new_identity.release_id} as album "
                    f"{new.album_id}; nothing to retag"
                ),
            )
        return BeetsRetagResult(
            outcome=RETAG_NOT_HELD,
            detail=(
                f"library holds neither {old_identity.release_id} nor "
                f"{new_identity.release_id}"
            ),
        )

    if isinstance(new, CurrentBeetsUnique):
        # The double-sided merge: two installed albums MusicBrainz now calls
        # one release. Merging or deleting either is the operator's call, never
        # this code's (issue #1059 invariant 5 — this is a curated collection,
        # and 0 live instances exist). Retagging would ALSO collide the two
        # albums under one duplicate key.
        return BeetsRetagResult(
            outcome=RETAG_AMBIGUOUS,
            detail=(
                "library holds both sides of the merge: "
                f"{old_identity.release_id} as album {old.album_id} and "
                f"{new_identity.release_id} as album {new.album_id}; "
                "merging or deleting either is an operator decision"
            ),
        )

    query = mbsync_album_query(old_identity)
    try:
        run = run_mbsync(query)
    except Exception as exc:  # noqa: BLE001 - external edge, typed outcome
        mbsync_note = f"mbsync raised {type(exc).__name__}: {exc}"
        log.warning("mbsync for %s raised: %s", old_identity.release_id, exc)
    else:
        mbsync_note = f"mbsync exited {run.returncode}"
        if run.returncode != 0:
            log.warning(
                "mbsync for %s exited %s: %s",
                old_identity.release_id, run.returncode, run.stderr.strip()[-500:],
            )

    # The exit status decided nothing; the re-read library does. This is the
    # whole reason the outcome is not derived from the subprocess: mbsync logs
    # and skips a release it cannot fetch and still exits 0.
    reresolved = _resolve_pair(beets, old_identity, new_identity)
    if isinstance(reresolved, str):
        return _failed(f"{mbsync_note}; {reresolved}")
    old_after, new_after = reresolved

    if isinstance(old_after, CurrentBeetsMissing) and isinstance(
        new_after, CurrentBeetsUnique,
    ):
        return BeetsRetagResult(
            outcome=RETAG_RETAGGED,
            detail=(
                f"retagged album {new_after.album_id} from "
                f"{old_identity.release_id} to {new_identity.release_id} "
                f"({mbsync_note})"
            ),
        )

    return _failed(
        f"{mbsync_note}, but the library did not move: "
        f"{old_identity.release_id} is {_describe(old_after)}; "
        f"{new_identity.release_id} is {_describe(new_after)}"
    )


__all__ = [
    "MBSYNC_NOMOVE_FLAG",
    "MBSYNC_TIMEOUT_SECONDS",
    "RETAG_ALREADY_CURRENT",
    "RETAG_AMBIGUOUS",
    "RETAG_FAILED",
    "RETAG_NOT_HELD",
    "RETAG_READY_OUTCOMES",
    "RETAG_RETAGGED",
    "BeetsRetagResult",
    "CurrentReleaseResolver",
    "MbsyncFn",
    "MbsyncRun",
    "RetagOutcome",
    "mbsync_album_query",
    "retag_merged_album",
    "run_beets_mbsync",
]
