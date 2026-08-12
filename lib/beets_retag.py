"""Follow a MusicBrainz merge on the Beets side, at exactly one album
(#1059/#1087).

When MusicBrainz merges release A into release B, the installed album still
carries A in ``mb_albumid``. The request cannot simply be rekeyed to B first:
Beets keys album duplicate detection on ``mb_albumid``
(``duplicate_keys: album: [mb_albumid, discogs_albumid]``, combined as an
``AndQuery`` in ``beets/library/models.py::duplicates_query``), so an incoming
album at B would not match a library album filed at A. No duplicate would be
flagged, the import would land a SECOND album, and the existing-album lookup
would miss — routing the quality decision through ``import_no_exist`` and
silently skipping the downgrade guard on exactly the albums we already hold.

So the library moves first. This module runs one ``beet modify`` invocation
under an anchored query on the old ID, and then re-reads the library to
decide what actually happened. Only when the observable end state is "the old
ID is gone and the new ID is uniquely held" may the caller rekey the request.

**Why not ``mbsync``.** ``mbsync`` is Beets' own "MusicBrainz changed its
mind" command, and #1059 / PR #1075 built this module on it. It cannot follow
a release-only merge: ``beetsplug/mbsync.py::albums()`` maps library items
onto the fetched release's tracks using exactly two keys —
``mb_releasetrackid``, then ``mb_trackid`` — both of which routinely change
when MusicBrainz merges the RELEASE but not the underlying RECORDINGs. An
empty item-to-track mapping means ``AlbumMatch.apply_metadata`` iterates
nothing, ``changed`` stays False, and ``mbsync`` exits 0 having moved nothing
(issue #1087; verified empirically against the live library — 0/10 items
mapped). ``beet modify`` needs no candidate mapping at all: it sets one field
by query. It has no equivalent failure mode, and it makes no network call.

Five properties are load-bearing:

* **The query names one album.** :func:`retag_album_query` anchors the regex
  with ``^`` and ``\\Z`` — NOT a trailing ``$`` — so it can only ever name
  albums filed under exactly the old ID. ``beet modify`` retags everything
  its query matches, so an unanchored or substring query is the difference
  between one album and part of the library; ``$`` would additionally have
  matched a stored value carrying one trailing newline, which ``\\Z`` does
  not (#1087 review; see the function docstring for the mechanism).
* **``-a`` targets Albums, not Items — and that is what makes the identity
  move on both.** ``beets/ui/commands/modify.py::modify_parse_args``
  classifies each argument by CONTENT, not position: a token is an
  assignment iff it contains ``=`` and the text before the first ``=``
  contains no ``:``. The anchored query contains ``:`` and no ``=`` (a query
  token); ``mb_albumid=<new-id>`` contains no ``:`` before its ``=`` (an
  assignment). Argument order is therefore irrelevant. ``-a`` selects
  ``library.Album`` as ``modify_items``'s query target, and
  ``Album.try_sync(write, move, inherit)`` calls ``Album.store(inherit=True)``
  (the default; inherit is only off with ``-I``), which fans every
  inheritable FIXED attribute out to every item and stores it —
  ``mb_albumid`` is one, via ``Album.item_keys``
  (``beets/library/models.py:593-628``). Drop ``-a`` and the query instead
  matches ITEMS directly: each item's own ``mb_albumid`` moves, the ALBUM
  row's does not, and the library is left in exactly the split state this
  module exists to prevent.
* **Identity only — never a tag write, never a file move.** ``-W``
  (``--nowrite``) and ``-M`` (``--nomove``) are both explicit ``False``
  overrides (``beets/ui/__init__.py::should_write`` / ``should_move``) that
  beat the ``import.write`` / ``import.move`` config fallback our contract
  pins to ``yes`` — so this command never touches a tag on disk and never
  moves a file. ``-W`` is deliberate, not merely cautious: the post-retag
  guard reads the Beets DB, so the primitive's effect surface must equal the
  guard's observation surface. A tag WRITE is N per-file operations whose
  partial failure could leave some files re-taggable and some not while the
  DB already says the retag landed — a divergence the guard would report as
  success. ``-W`` makes the effect exactly one ``Album.store()`` transaction:
  it lands or it doesn't.
* **``modify``'s exit status is not evidence.** A query that matches NOTHING
  raises ``UserError("No matching albums found.")``
  (``beets/ui/commands/utils.py::do_query``), which the CLI entry point maps
  to exit 1 — not 0. The genuine exit-0-without-movement case is a query
  that MATCHES but produces no field change: ``modify_items`` prints "No
  changes to make." and returns, still exit 0. Either way, a subprocess exit
  code read against a shared SQLite file another process can concurrently
  mutate is never itself an observation of the end state — the re-read
  library is.
* **An unreadable Beets authority is never absence.** A resolver that omits
  an identity, or raises, is a failure — never "the album is not held".
  Reading it as absence would authorize a rekey that manufactures a
  duplicate pressing.

Nothing here is durable. A failure of THIS module leaves the library and the
request exactly as they were, and the next sweep re-derives the same world
(invariant 11 — broken worlds surface and restart; nothing is parked).

That is a statement about the retag, NOT about the composite. A successful
``retagged`` outcome IS durable — the library has moved — and the caller's
subsequent rekey can still be refused, leaving the installed album at the
survivor and the request at the merged-away id. That composite is the
caller's to prevent and record, and ``lib/download_validation.py`` does both:
it reads the survivor's occupants before asking for a retag, and audits the
residual race (:data:`lib.download_validation.MERGE_REKEY_BLOCKED` and
``MergeRekeyOutcome.split_identity``).
"""

from __future__ import annotations

import logging
import re
import subprocess as sp
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal, Protocol

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity

if TYPE_CHECKING:
    from lib.config import CratediggerConfig

log = logging.getLogger("cratedigger")

#: ``beet modify`` makes no network call, so this bound exists only so a
#: wedged subprocess launch cannot stall a whole sweep.
RETAG_TIMEOUT_SECONDS = 120

#: ``-a`` selects Albums as the query target. Without it ``modify`` targets
#: Items by default, and the ALBUM row's ``mb_albumid`` never moves — see
#: the module docstring for what that leaves behind.
RETAG_ALBUM_FLAG: Final = "-a"

#: ``-M`` is ``--nomove``: ``beets/ui/commands/modify.py`` declares it as
#: ``action="store_false", dest="move"``, and ``beets/ui/__init__.py::
#: should_move`` returns that explicit ``False`` instead of falling back to
#: ``import.move or import.copy`` (which our config contract pins to True).
#: Belt-and-braces, not something the current config makes reachable:
#: ``mb_albumid`` is in no path template, so retagging it alone cannot
#: itself relocate a file today. Kept because a future path-template or
#: config change that made ``mb_albumid`` path-relevant would otherwise
#: silently start reorganising the library under a merge follow, and there
#: is no cost to keeping the flag now.
RETAG_NOMOVE_FLAG: Final = "-M"

#: ``-W`` is ``--nowrite``: forces ``should_write`` to return ``False``
#: regardless of ``import.write``. Load-bearing per the module docstring —
#: the retag's effect surface must equal the post-retag guard's DB-only
#: observation surface.
RETAG_NOWRITE_FLAG: Final = "-W"

#: ``-y`` skips ``modify``'s interactive confirmation prompt. Without it a
#: subprocess with no attached terminal hangs until the timeout.
RETAG_YES_FLAG: Final = "-y"

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
class ModifyRetagRun:
    """What one ``beet modify`` retag invocation reported.

    Kept for the diagnostic detail only. ``returncode`` is deliberately NOT a
    decision input: a query matching nothing exits 1 (``UserError``), but a
    query that MATCHES and produces no field change still prints "No changes
    to make." and exits 0 — and either way, an exit code read against a
    shared SQLite file another process can concurrently mutate is not itself
    evidence of the end state.
    """

    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class BeetsRetagResult:
    """The one typed answer this module gives, plus operator-facing detail."""

    outcome: RetagOutcome
    detail: str


#: The injected ``beet modify`` runner. It is a definition-time default on
#: :func:`retag_merged_album`, so tests pass a replacement explicitly and
#: never patch the module binding — patching does not replace a captured
#: default (`.claude/rules/code-quality.md` § mocks, strategy 2).
type RetagModifyFn = Callable[[str, str], ModifyRetagRun]

type SubprocessRunFn = Callable[..., sp.CompletedProcess[bytes]]


class MergeRetagFn(Protocol):
    """Exact injection contract for the one-album library retag.

    Declared here rather than at a call site because BOTH import lanes that
    can follow a MusicBrainz merge inject it: the automation validation seam
    and the force-import dispatch entry point (#1080). One contract, so the
    two callers cannot drift into two shapes.
    """

    def __call__(
        self,
        cfg: CratediggerConfig,
        *,
        old_identity: ReleaseIdentity,
        new_identity: ReleaseIdentity,
    ) -> BeetsRetagResult: ...


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


def retag_album_query(identity: ReleaseIdentity) -> str:
    """The anchored Beets query naming exactly the album filed under ``identity``.

    ``beet modify`` retags everything the query matches, so the regex is
    anchored: ``mb_albumid::^<escaped-id>\\Z`` cannot match a longer id that
    merely contains this one. An unanchored or substring query is the
    difference between retagging one album and retagging part of the
    library.

    The end anchor is ``\\Z``, never a bare ``$``. Beets compiles the
    pattern with plain ``re.compile`` (no flags) and matches via
    ``pattern.search()`` — both the Python fallback and the SQLite
    ``regexp()`` UDF (``beets/dbcore/query.py::RegexpQuery``,
    ``beets/dbcore/db.py::Database.add_functions``). Without ``re.MULTILINE``,
    a trailing ``$`` matches at the true end of the string OR immediately
    before ONE trailing newline — so ``^<old-id>$`` would additionally match
    an ``mb_albumid`` of ``"<old-id>\\n"`` on an unrelated album, silently
    retagging it onto an id that is not its own (#1087 review; reproduced
    live). ``\\Z`` matches only the true end of the string, so it does not.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "retag query is MusicBrainz-only; refusing to build a query for "
            f"{identity.source} release {identity.release_id}"
        )
    return f"mb_albumid::^{re.escape(identity.release_id)}\\Z"


def retag_assignment(identity: ReleaseIdentity) -> str:
    """The ``field=value`` token naming the survivor identity.

    ``modify_parse_args`` classifies any argument containing ``=`` whose text
    before the first ``=`` contains no ``:`` as an assignment — never a
    query token — so this and :func:`retag_album_query` cannot be confused
    for one another regardless of argv order. The value is template-evaluated
    (``functemplate.template``) before being stored; MusicBrainz UUIDs
    contain no ``$``/``%`` so they are inert, but this function only ever
    accepts an already-validated :class:`ReleaseIdentity` — never raw text.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "retag assignment is MusicBrainz-only; refusing to build an "
            f"assignment for {identity.source} release {identity.release_id}"
        )
    return f"mb_albumid={identity.release_id}"


def run_beets_modify_retag(
    query: str,
    assignment: str,
    *,
    runner: SubprocessRunFn = sp.run,
    timeout: int = RETAG_TIMEOUT_SECONDS,
) -> ModifyRetagRun:
    """Run ``beet modify -a -M -W -y`` for one query in the deployment-supplied
    Beets runtime.

    Invoked as ``<beets python> -m beets modify -a -M -W -y <query>
    <assignment>``: ``python -m beets`` is a valid entry point in the pinned
    2.13.1, and depending on a ``beet`` binary being on this process's PATH
    would silently pick up whatever beets the invoking user happens to have.
    The interpreter and environment come from
    ``lib/util.py::beets_subprocess_env`` — the single source of truth for
    how a beets subprocess finds its config and interpreter.

    Every flag here is load-bearing; see the module docstring for why. This
    command makes no network call and needs no candidate mapping, unlike the
    ``mbsync`` primitive it replaces.

    Raises on a launch/timeout failure; :func:`retag_merged_album` turns that
    into a typed outcome after re-reading the library.
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env.get("CRATEDIGGER_BEETS_PYTHON", "")
    if not python:
        raise RuntimeError("CRATEDIGGER_BEETS_PYTHON is not configured")
    proc = runner(
        [
            python, "-m", "beets", "modify",
            RETAG_ALBUM_FLAG, RETAG_NOMOVE_FLAG, RETAG_NOWRITE_FLAG,
            RETAG_YES_FLAG, query, assignment,
        ],
        capture_output=True,
        timeout=timeout,
        env=env,
    )
    return ModifyRetagRun(
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
    run_modify: RetagModifyFn = run_beets_modify_retag,
) -> BeetsRetagResult:
    """Move the one installed album from ``old_identity`` to ``new_identity``.

    Returns an outcome in :data:`RETAG_READY_OUTCOMES` only when the library
    is observably at the new ID or holds neither — that, and nothing else,
    authorizes the caller to rekey the request.

    ``run_modify`` is a definition-time default: tests INJECT a replacement,
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
            "retag is MusicBrainz-only; refusing "
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
        # one release. Merging or deleting either is the operator's call (issue
        # #1059 invariant 5 — this is a curated collection, and 0 live
        # instances exist). Retagging would ALSO collide the two albums under
        # one duplicate key.
        return BeetsRetagResult(
            outcome=RETAG_AMBIGUOUS,
            detail=(
                "library holds both sides of the merge: "
                f"{old_identity.release_id} as album {old.album_id} and "
                f"{new_identity.release_id} as album {new.album_id}; "
                "merging or deleting either is an operator decision"
            ),
        )

    query = retag_album_query(old_identity)
    assignment = retag_assignment(new_identity)
    try:
        run = run_modify(query, assignment)
    except Exception as exc:  # noqa: BLE001 - external edge, typed outcome
        modify_note = f"beet modify raised {type(exc).__name__}: {exc}"
        log.warning("beet modify for %s raised: %s", old_identity.release_id, exc)
    else:
        modify_note = f"beet modify exited {run.returncode}"
        if run.returncode != 0:
            log.warning(
                "beet modify for %s exited %s: %s",
                old_identity.release_id, run.returncode, run.stderr.strip()[-500:],
            )

    # The exit status decided nothing; the re-read library does. A query
    # matching nothing exits 1 (UserError); a query that matches but changes
    # nothing prints "No changes to make." and exits 0 — and either way, an
    # exit code read against a shared SQLite file another process can
    # concurrently mutate is not itself evidence of the end state.
    reresolved = _resolve_pair(beets, old_identity, new_identity)
    if isinstance(reresolved, str):
        return _failed(f"{modify_note}; {reresolved}")
    old_after, new_after = reresolved

    if isinstance(old_after, CurrentBeetsMissing) and isinstance(
        new_after, CurrentBeetsUnique,
    ):
        return BeetsRetagResult(
            outcome=RETAG_RETAGGED,
            detail=(
                f"retagged album {new_after.album_id} from "
                f"{old_identity.release_id} to {new_identity.release_id} "
                f"({modify_note}); -W left file tags on disk still naming "
                f"{old_identity.release_id} until a successful import "
                "writes them"
            ),
        )

    return _failed(
        f"{modify_note}, but the library did not move: "
        f"{old_identity.release_id} is {_describe(old_after)}; "
        f"{new_identity.release_id} is {_describe(new_after)}"
    )


__all__ = [
    "RETAG_ALBUM_FLAG",
    "RETAG_ALREADY_CURRENT",
    "RETAG_AMBIGUOUS",
    "RETAG_FAILED",
    "RETAG_NOMOVE_FLAG",
    "RETAG_NOT_HELD",
    "RETAG_NOWRITE_FLAG",
    "RETAG_READY_OUTCOMES",
    "RETAG_RETAGGED",
    "RETAG_TIMEOUT_SECONDS",
    "RETAG_YES_FLAG",
    "BeetsRetagResult",
    "CurrentReleaseResolver",
    "MergeRetagFn",
    "ModifyRetagRun",
    "RetagModifyFn",
    "RetagOutcome",
    "retag_album_query",
    "retag_assignment",
    "retag_merged_album",
    "run_beets_modify_retag",
]
