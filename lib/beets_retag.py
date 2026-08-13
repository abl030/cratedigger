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
under an exact-match query on the old ID, and then re-reads the library to
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

* **The query and the post-retag guard select by the SAME mechanism, AND the
  query pins the exact row the guard already resolved.** The query is an AND
  of two exact-match clauses: ``id:=<album_id>`` — the ALBUM PRIMARY KEY the
  guard's pre-check just resolved — and ``mb_albumid:=<old-id>``, Beets'
  ``=`` prefix, NOT a regex. Both compile to ``dbcore.query.MatchQuery`` and
  emit plain SQL equality — ``(albums.id = ?) and (albums.mb_albumid = ?)``
  (``beets/library/queries.py``, ``beets/dbcore/query.py::
  MatchQuery.col_clause``) — the identical comparison
  :meth:`lib.beets_db.BeetsDB.resolve_current_releases` already performs
  when it re-reads the library (a TEXT-equality membership test), so the
  query and the guard select by one mechanism, not two that can silently
  disagree. Before #1093 the query was an anchored regex
  (``mb_albumid::^<id>\\Z``) evaluated by Beets' SQLite ``regexp()`` UDF,
  which DECODES a BLOB-stored value before matching
  (``beets/dbcore/db.py``) — so a BLOB-stored ``mb_albumid`` (only
  reachable via a third-party raw-SQL writer; Beets itself always writes
  ``str``) could match the regex query while staying invisible to the
  guard's exact-equality comparison. Live-verified against a real BLOB
  write: the retired regex form matched it, the exact-match form and the
  guard now both report it absent (#1093 item 2).

  Storage-shape agreement alone still leaves a TIME-OF-CHECK/TIME-OF-USE
  gap: the guard resolves the row on ONE connection, then ``beet modify``
  re-selects BY VALUE on a SEPARATE connection at a LATER time. If a second
  album lands at the old id in that window, a value-only query would retag
  BOTH — the guard counted one, but the query matches whatever holds the
  value NOW. The ``id:=<album_id>`` clause closes this: it pins the exact
  ROWID the guard authorized (``albums.id`` is ``INTEGER PRIMARY KEY``
  with no ``AUTOINCREMENT`` — live-verified against the real schema — so
  it is a bare SQLite rowid, reusable by a later insert after a delete;
  the id clause pins that rowid, not a row identity guaranteed unique
  forever), so a second album at the same value is never
  touched by this execution regardless of what the value-only clause would
  have matched. The ``mb_albumid:=<old-id>`` clause is not redundant with
  it — dropping it would turn a conditional retag into a blind write:
  without the value compare-and-set, a row whose ``mb_albumid`` has ALREADY
  moved off the old id since the guard's read (retagged by a concurrent
  actor, or simply raced onto the survivor already) would be overwritten
  anyway, based on identity alone. Live-verified against a real
  ``beet modify`` subprocess, three ways: the correct id AND the correct
  value retags; the correct value but a WRONG id (a different album now
  holding the guard-resolved id) is refused (``No matching albums found.``,
  the row is untouched); the correct id but a CHANGED value (something else
  retagged this exact album since the guard's read) is refused the same
  way. Both clauses are load-bearing; keeping only one reopens the harm the
  other half exists to prevent (#1093 review residual).
* **``-a`` targets Albums, not Items — and that is what makes the identity
  move on both.** ``beets/ui/commands/modify.py::modify_parse_args``
  classifies each argument by CONTENT, not position: a token is an
  assignment iff it contains ``=`` and the text before the first ``=``
  contains no ``:``. Both exact-match query tokens contain BOTH ``:`` and
  ``=`` (``id:=<album_id>``, ``mb_albumid:=<old-id>``), but each one's
  ``:`` still precedes its first ``=``, so both stay query tokens;
  ``mb_albumid=<new-id>`` contains no ``:`` before its ``=`` (an
  assignment). Argument order is therefore irrelevant. ``-a`` selects
  ``library.Album`` as ``modify_items``'s query target, and
  ``Album.try_sync(write, move, inherit)`` calls ``Album.store(inherit=True)``
  (the default; inherit is only off with ``-I``), which fans every
  inheritable FIXED attribute out to every item and stores it —
  ``mb_albumid`` is one, via ``Album.item_keys``
  (``beets/library/models.py:593-628``). Drop ``-a`` and the query instead
  matches ITEMS directly — and the ``id:=<album_id>`` clause then binds to
  the ITEMS table's own, independent primary key namespace rather than the
  album's: only whichever item's OWN id happens to coincide with the
  guard-resolved album id can move at all (live-verified, deterministic in
  the T6 fixture: a single coincidental collision, not every item). Either
  way the ALBUM row's ``mb_albumid`` does not move, and the library is left
  in exactly the split state this module exists to prevent.
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
#: is no cost to keeping the flag now. Empirically proven, not merely
#: asserted (#1093 item 4): under a fixture path template that DOES
#: include ``$mb_albumid``, dropping this flag genuinely relocates the
#: file and lets ``prune_dirs`` sweep the vacated directory's clutter —
#: see ``TestRealModifyRetagRelocationAndSidecarClausesAreReachable`` in
#: ``tests/test_beets_retag.py``.
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


#: The injected ``beet modify`` runner. ``query_tokens`` is the compound
#: query :func:`retag_album_query` returns — the ``id:=`` and
#: ``mb_albumid:=`` clauses, as SEPARATE argv elements (Beets ANDs distinct
#: query tokens implicitly; joining them into one string would not). It is
#: a definition-time default on :func:`retag_merged_album`, so tests pass a
#: replacement explicitly and never patch the module binding — patching
#: does not replace a captured default (`.claude/rules/code-quality.md` §
#: mocks, strategy 2).
type RetagModifyFn = Callable[[tuple[str, str], str], ModifyRetagRun]

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


def retag_album_query(
    identity: ReleaseIdentity, *, album_id: int,
) -> tuple[str, str]:
    """Two ANDed exact-match Beets query tokens naming precisely the ONE
    album row the guard just resolved — and nothing else, by the same
    SQL-equality mechanism the post-retag guard uses to read the library
    back, AND pinned to the exact primary key the guard resolved rather
    than re-selected by value alone (#1093).

    Token 1, ``id:=<album_id>``, pins the exact row the pre-check's
    ``CurrentBeetsUnique.album_id`` named. Token 2,
    ``mb_albumid:=<old-id>``, is Beets' ``=`` query prefix — NOT a regex —
    keeping the identity compare-and-set: a row whose ``mb_albumid`` has
    already moved off ``identity`` since the guard's read is not blindly
    overwritten by primary key alone. Beets ANDs separate argv query
    tokens implicitly (``dbcore.parse_sorted_query``), so passing both as
    SEPARATE argv elements (never one joined string) is load-bearing.

    ``beets/library/queries.py::parse_query_parts`` maps prefix ``=`` to
    ``dbcore.query.MatchQuery`` for BOTH tokens (before #1093 the
    ``mb_albumid`` token used the ``:`` prefix, mapped to ``RegexpQuery``,
    with the pattern anchored ``^<escaped-id>\\Z``, and there was no ``id``
    token at all). ``MatchQuery.col_clause()`` (``beets/dbcore/query.py``)
    emits plain SQL equality for each — live-verified compiled clause:
    ``(albums.id = ?) and (albums.mb_albumid = ?)`` — the identical
    comparison :meth:`lib.beets_db.BeetsDB.resolve_current_releases`
    performs when it re-reads the library (``a.mb_albumid IN (SELECT ...
    FROM json_each(?))``, itself a TEXT-equality membership test). One
    selection mechanism for one operation, rather than two
    independently-correct ones that could silently disagree: live-verified,
    the ``mb_albumid:=`` token alone selects only the exact target out of a
    same-prefix decoy (``<id>0``, which a substring/prefix query would also
    match), and does NOT select a BLOB-stored value the retired regex form
    could see via the ``regexp()`` UDF's byte-decoding
    (``beets/dbcore/db.py``) but the guard could not (only reachable via a
    third-party raw-SQL writer; Beets itself always writes ``str``).

    The compound form closes the residual TIME-OF-CHECK/TIME-OF-USE gap a
    value-only query leaves: the guard resolves the row on one connection,
    then ``beet modify`` re-selects by value on a separate connection at a
    later time. Live-verified against a real subprocess, three ways: the
    correct id AND value together retag; the correct value with a WRONG id
    (a different album now holding the guard-resolved id) is refused (``No
    matching albums found.``, untouched); the correct id with a CHANGED
    value (something else retagged this exact album since the guard's
    read) is refused the same way — proving the ``id`` clause alone is
    unsafe (it would convert this into a blind write) and the ``mb_albumid``
    clause alone is unsafe (it would retag whoever now holds the value, not
    only the row the guard authorized).

    ``modify_parse_args`` (``beets/ui/commands/modify.py``) classifies both
    tokens by CONTENT, not position: each contains ``=``, but the text
    before its first ``=`` (``"id:"``, ``"mb_albumid:"``) still contains
    ``:``, so both fall to the ``else: query.append(arg)`` branch — QUERY
    tokens, never assignments — verified against the real parser, live,
    not merely read.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "retag query is MusicBrainz-only; refusing to build a query for "
            f"{identity.source} release {identity.release_id}"
        )
    return (f"id:={album_id}", f"mb_albumid:={identity.release_id}")


def retag_assignment(identity: ReleaseIdentity) -> str:
    """The ``field=value`` token naming the survivor identity.

    ``modify_parse_args`` classifies any argument containing ``=`` whose text
    before the first ``=`` contains no ``:`` as an assignment — never a
    query token. This and both tokens :func:`retag_album_query` returns
    contain ``=`` (the query tokens as their exact-match prefix, this as its
    assignment operator), but each query token's colon always precedes its
    ``=``, so all three remain unambiguous regardless of argv order. The
    value is template-evaluated (``functemplate.template``) before being
    stored; MusicBrainz UUIDs contain no ``$``/``%`` so they are inert, but
    this function only ever accepts an already-validated
    :class:`ReleaseIdentity` — never raw text.
    """
    if identity.source != "musicbrainz":
        raise ValueError(
            "retag assignment is MusicBrainz-only; refusing to build an "
            f"assignment for {identity.source} release {identity.release_id}"
        )
    return f"mb_albumid={identity.release_id}"


def run_beets_modify_retag(
    query_tokens: tuple[str, str],
    assignment: str,
    *,
    runner: SubprocessRunFn = sp.run,
    timeout: int = RETAG_TIMEOUT_SECONDS,
) -> ModifyRetagRun:
    """Run ``beet modify -a -M -W -y`` for the compound query in the
    deployment-supplied Beets runtime.

    Invoked as ``<beets python> -m beets modify -a -M -W -y <id-token>
    <mb_albumid-token> <assignment>`` — the two query tokens are SEPARATE
    argv elements, never joined into one string, because Beets ANDs
    distinct argv query tokens implicitly; a joined string would parse as
    one malformed token instead. ``python -m beets`` is a valid entry point
    in the pinned 2.13.1, and depending on a ``beet`` binary being on this
    process's PATH would silently pick up whatever beets the invoking user
    happens to have. The interpreter and environment come from
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
            RETAG_YES_FLAG, *query_tokens, assignment,
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

    # `old` is CurrentBeetsUnique here — every other pre-state returned
    # above. Its album_id is what the compound query pins (#1093 review
    # residual): the id clause names the EXACT ROWID this resolution
    # authorized (`albums.id` is INTEGER PRIMARY KEY with no
    # AUTOINCREMENT, so it is a bare, reusable-after-delete SQLite rowid,
    # not a row identity guaranteed unique forever), closing the gap a
    # value-only re-select would leave open to a second album landing at
    # old_identity between this read and the subprocess launch.
    query_tokens = retag_album_query(old_identity, album_id=old.album_id)
    assignment = retag_assignment(new_identity)
    try:
        run = run_modify(query_tokens, assignment)
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

    # Three honest shapes for what "not ready" can mean here (#1093 item 5,
    # review round 2). `old` is CurrentBeetsUnique — every other pre-state
    # returned above — so its album_id is the one album THIS execution was
    # retagging.
    #
    # "did not move" is true ONLY when old_after is STILL Unique at the
    # SAME album_id: nothing happened to the row this execution targeted.
    # A DIFFERENT album now occupying old_identity (old_after.album_id !=
    # old.album_id) is a distinct shape — the original occupant is gone,
    # but old_identity is still held, just by someone else — never "did not
    # move" and never "moved off" (the id itself is not gone).
    #
    # "moved off" is true ONLY when old_after is CurrentBeetsMissing — the
    # id matches ZERO album rows. CurrentBeetsAmbiguous is NEVER "moved
    # off": every CurrentBeetsAmbiguous reason
    # (`multiple_matches`/`conflicting_identity`/`empty_topology`/
    # `invalid_path`/`unresolved_relative_path`/`split_topology`) requires
    # `resolve_current_releases` to have found at least one matching album
    # row — `album_ids` is only ever empty for CurrentBeetsMissing
    # (`lib/beets_db.py::resolve_current_releases`). Two distinct
    # self-contradictions have shipped from conflating these: round 1 (the
    # bug the "did not move" branch above fixes) said "...the library did
    # not move: <old> is not held; <new> is ambiguous across 7, 8" — wrong
    # because old_after WAS Missing there, genuine evidence of a move.
    # Round 2 (this branch) is the SAME conflation from the other
    # direction: this module's first fix pass unconditionally said "moved
    # off" whenever old_after was not Unique, which is equally wrong when
    # old_after is Ambiguous — e.g. a concurrent writer lands a SECOND
    # album at old_identity, `modify` moves nothing, and old_after reads
    # Ambiguous(multiple_matches, album_ids=(7, 9)) — the id is still held
    # (by both albums, including the original 7), never gone.
    if (
        isinstance(old_after, CurrentBeetsUnique)
        and old_after.album_id == old.album_id
    ):
        # #1093 round 3 review F-4: the subject is the ROW THIS EXECUTION
        # TARGETED, matching the comment above (not "the library" — a
        # concurrent writer can independently move new_identity between
        # the pre-check and this re-read, e.g. landing it uniquely at a
        # different album entirely, while this row stays put; "the
        # library did not move" would then contradict its own trailer the
        # moment new_after names that different album).
        return _failed(
            f"{modify_note}, but the row this execution targeted did not "
            f"move: {old_identity.release_id} is {_describe(old_after)}; "
            f"{new_identity.release_id} is {_describe(new_after)}"
        )
    if isinstance(old_after, CurrentBeetsMissing):
        return _failed(
            f"{modify_note}; the library moved off {old_identity.release_id} "
            "but did not land at a state the caller may rekey onto: "
            f"{old_identity.release_id} is now {_describe(old_after)}; "
            f"{new_identity.release_id} is {_describe(new_after)}"
        )
    if isinstance(old_after, CurrentBeetsUnique):
        return _failed(
            f"{modify_note}; {old_identity.release_id} changed occupant: "
            f"was album {old.album_id}, is now {_describe(old_after)}; "
            f"{new_identity.release_id} is {_describe(new_after)}"
        )
    return _failed(
        f"{modify_note}; {old_identity.release_id} is still held but "
        f"{_describe(old_after)}; {new_identity.release_id} is "
        f"{_describe(new_after)}"
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
