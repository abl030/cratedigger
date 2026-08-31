"""Generated properties for the one-album ``beet modify`` retag
(#1059/#1087/#1093).

The pins in ``tests/test_beets_retag.py`` prove the exact branches; these
properties patrol the world space around them, driving the REAL
``retag_merged_album`` over every combination of (old-side resolution ×
new-side resolution × what ``beet modify`` does × the library it leaves
behind).

Why this surface earns a property: the retag is the PRECONDITION for rekeying
a request. Every ready outcome authorizes moving ``album_requests.mb_release_id``
onto the survivor, and a ready outcome returned while the library is still
filed under the merged-away id manufactures exactly the duplicate-pressing
state invariant 5 protects — the import lands a SECOND album, and the
existing-album lookup misses so the quality decision routes through
``import_no_exist`` and silently skips the downgrade guard.

Invariants patrolled — each is a module-level checker so the known-bad
self-tests below can call it directly:

G1  A ready outcome is returned ONLY when the library is observably at the
    new id (old missing, new unique) or holds neither id. Nothing else may
    authorize a rekey.
G2  ``beet modify`` is invoked at most once, and only in the single world
    that authorizes a library mutation: the old id uniquely held and the
    new id not held at all. An ambiguous or absent old side, or an
    already-present new side, must never reach it.
G3  The query handed to ``modify`` always names the OLD identity (both by
    the guard-resolved primary key and the identity value) and the
    assignment always names the NEW identity — never the other way, and
    never each other. Retagging the survivor is a no-op at best and a
    wrong-album mutation at worst. This is a CALL-ARGUMENT seam check
    (recomputes ``retag_album_query``/``retag_assignment`` and compares) —
    legitimate for that narrow claim, but NOT evidence that
    ``retag_album_query``'s own selection mechanism agrees with the guard's;
    see M1 below for that.
G4  Both sides held never returns a ready outcome. Two installed albums that
    MusicBrainz now calls one release is the operator's decision.
G5  (#1093 item 5, both review rounds) A ``failed`` detail's own words must
    match what the re-read library actually shows, in every direction:
    "did not move" only when the old id is STILL held by the SAME album;
    "moved off" only when the old id is observably GONE (Missing, never
    Ambiguous — every Ambiguous reason still requires a matching row);
    "changed occupant" only when a DIFFERENT album now holds it. Each
    direction has shipped as a real, reachable self-contradiction at least
    once.
M1  (#1093 item 2, review F2 + round 3 F1) The retag query's compiled SQL
    clause and the post-retag guard's own matching SQL
    (``BeetsDB.resolve_current_release`` — the exact method
    ``retag_merged_album`` re-reads the library with, not the unrelated
    ``_matching_album_ids``) select the SAME row for every ``mb_albumid``
    storage shape a raw third-party writer could produce, crossed against
    every case the QUERIED identity itself could take — computed via two
    INDEPENDENTLY executed SQL statements against the same real
    beets-schema data, never by recomputing one mechanism and comparing it
    to itself. G3 cannot stand in for this: recomputing ``retag_album_query``
    on both sides of a comparison can never fail on a change to what that
    function selects.

``TestRealModifyRetagOverItemCountBoundaries`` below is NOT a generated
property, and does not claim to be. #1075 DID ship a real-subprocess test
(``TestRealMbsyncMovesIdentityNotFiles``), so a real subprocess is not what
was missing; its fixture modelled a RECORDING-PRESERVING merge, so the
predecessor primitive's item-to-track mapping matched and the merge it
cannot actually follow — a RELEASE-ONLY one — never ran. The lesson: a real
subprocess is necessary, not sufficient; it must run over a world shaped
like the failure. That class is composed with the T6/T7 deterministic pins
over three hand-reasoned item-count equivalence classes (0 / 1 / 2) — see
the class docstring for why that partition is honest as pins, not claimed
as an independently certified generated domain. Every genuinely
combinatorial property (cardinality × modify-result × post-state, G1–G5)
runs through Hypothesis via ``TestRetagProperties``; M1 is a narrower,
independent property over generated ``mb_albumid`` storage shapes and runs
via ``TestQueryAndGuardConvergeOnStorageShape``.
"""

from __future__ import annotations

import logging
import sqlite3
import subprocess as sp
import tempfile
import unittest
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from beets import library as beets_library
from beets.dbcore.query import CollectionQuery
from beets.library.queries import parse_query_parts
from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_child import BeetsChildRun
from lib.beets_db import (
    BeetsDB,
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.beets_retag import (
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_READY_OUTCOMES,
    RETAG_RETAGGED,
    BeetsRetagResult,
    RetagOutcome,
    retag_album_query,
    retag_assignment,
    retag_merged_album,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB
from tests.test_beets_retag import (
    ITEM_COUNTS,
    MERGED,
    NEW,
    OLD,
    SIDECAR_NAME,
    SURVIVOR,
    RealModifyObservation,
    check_real_modify_retag_moved_every_identity,
    check_real_modify_retag_refuses_empty_topology,
    library,
    observe_real_modify_retag,
)


@contextmanager
def _silence_logs() -> Iterator[None]:
    """Every failing world logs a warning; hundreds of examples do not."""
    previous_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous_level)


# ---------------------------------------------------------------------------
# Invariant checkers — module level so the known-bad self-tests can call them
# ---------------------------------------------------------------------------

#: The library shapes that genuinely authorize a rekey: the new id uniquely
#: held with the old one gone, or neither id held at all.
def library_is_rekeyable(
    old_after: CurrentBeetsResolution,
    new_after: CurrentBeetsResolution,
) -> bool:
    """Whether the FINAL observed library state permits rekeying."""
    if not isinstance(old_after, CurrentBeetsMissing):
        return False
    return isinstance(new_after, (CurrentBeetsUnique, CurrentBeetsMissing))


def check_ready_only_when_rekeyable(
    outcome: str,
    *,
    old_after: CurrentBeetsResolution,
    new_after: CurrentBeetsResolution,
) -> None:
    """G1 — a ready outcome must match the observable end state."""
    if outcome not in RETAG_READY_OUTCOMES:
        return
    if not library_is_rekeyable(old_after, new_after):
        raise AssertionError(
            f"outcome {outcome!r} authorizes a rekey, but the library still "
            f"reads old={type(old_after).__name__} "
            f"new={type(new_after).__name__} — rekeying now would land a "
            "second album on the next import"
        )


def check_modify_only_for_a_uniquely_held_old_id(
    calls: list[tuple[tuple[str, str], str]],
    *,
    old_before: CurrentBeetsResolution,
    new_before: CurrentBeetsResolution,
) -> None:
    """G2 — the library mutation runs at most once, and only when it may."""
    if len(calls) > 1:
        raise AssertionError(
            f"beet modify was invoked {len(calls)} times for one album: {calls!r}"
        )
    if not calls:
        return
    old_is_uniquely_held = isinstance(old_before, CurrentBeetsUnique)
    new_is_absent = isinstance(new_before, CurrentBeetsMissing)
    if not old_is_uniquely_held:
        raise AssertionError(
            "beet modify was invoked while the old id resolved "
            f"{type(old_before).__name__} — only a uniquely held old album "
            "may be retagged"
        )
    if not new_is_absent:
        raise AssertionError(
            "beet modify was invoked while the new id already resolved "
            f"{type(new_before).__name__} — retagging onto a held survivor "
            "would collide two albums under one duplicate key"
        )


def check_query_and_assignment_name_the_right_identity(
    calls: list[tuple[tuple[str, str], str]],
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
    old_before: CurrentBeetsResolution,
) -> None:
    """G3 — the query targets the album we are moving AWAY from (both the
    resolved primary key AND the identity value), and the assignment
    carries the identity we are moving TO. Never confused, never swapped.

    This is a CALL-ARGUMENT seam check — it recomputes
    :func:`retag_album_query`/:func:`retag_assignment` and compares, which
    is legitimate for its OWN narrow claim ("did the caller pass the
    builder's real output through unmodified") but is NOT, and cannot be,
    evidence that :func:`retag_album_query`'s own selection semantics agree
    with the guard's (`.claude/rules/code-quality.md` § "Agree by
    construction stops at the outermost real adapter" — recomputing the
    same function on both sides of a comparison can never fail on a change
    to that function). See ``TestQueryAndGuardConvergeOnStorageShape``
    below for the real, independently-computed mechanism-convergence
    property (#1093 item 2 review F2).
    """
    if not calls:
        return
    old_before_is_unique = isinstance(old_before, CurrentBeetsUnique)
    if not old_before_is_unique:
        raise AssertionError(
            "beet modify was invoked without a uniquely-held old id to pin "
            f"the query to: old_before={old_before!r}"
        )
    for query_tokens, assignment in calls:
        for query in query_tokens:
            if new_identity.release_id in query:
                raise AssertionError(
                    f"modify query names the survivor {new_identity.release_id}: "
                    f"{query!r}"
                )
        if query_tokens != retag_album_query(
            old_identity, album_id=old_before.album_id,
        ):
            raise AssertionError(
                "modify query is not the compound exact-match query for "
                f"the old identity {old_identity.release_id} pinned to "
                f"album {old_before.album_id}: {query_tokens!r}"
            )
        if old_identity.release_id in assignment:
            raise AssertionError(
                "modify assignment names the merged-away id "
                f"{old_identity.release_id}: {assignment!r}"
            )

        if assignment != retag_assignment(new_identity):
            raise AssertionError(
                "modify assignment is not the survivor assignment for "
                f"{new_identity.release_id}: {assignment!r}"
            )


def check_both_held_is_never_ready(
    outcome: str,
    *,
    old_before: CurrentBeetsResolution,
    new_before: CurrentBeetsResolution,
) -> None:
    """G4 — the double-sided merge is fail-closed, always."""
    both_held = isinstance(old_before, CurrentBeetsUnique) and isinstance(
        new_before, CurrentBeetsUnique,
    )
    if both_held and outcome in RETAG_READY_OUTCOMES:
        raise AssertionError(
            f"outcome {outcome!r} authorizes a rekey while the library holds "
            "BOTH sides of the merge; merging or deleting either album is an "
            "operator decision"
        )


def check_failure_detail_does_not_contradict_the_observed_move(
    outcome: str,
    detail: str,
    *,
    old_before: CurrentBeetsResolution,
    old_after: CurrentBeetsResolution,
    new_before: CurrentBeetsResolution,
    new_after: CurrentBeetsResolution,
) -> None:
    """G5 (#1093 item 5, both review rounds; round 3 F-4) — the failure
    detail's own words must match what ``old_after`` actually shows,
    checked in EVERY direction the production wording can claim:

    * "did not move" is true ONLY when ``old_after`` is STILL
      ``CurrentBeetsUnique`` at the SAME ``album_id`` ``old_before`` named
      — round 1's shipped bug claimed this whenever ``old_after`` was
      merely non-Unique, which is false the moment ``old_after`` shows the
      id already gone (Missing) or a DIFFERENT album now occupying it.
    * "moved off" is true ONLY when ``old_after`` is ``CurrentBeetsMissing``
      — round 1's first fix pass claimed this whenever ``old_after`` was
      not ``CurrentBeetsUnique`` at all, which is false for
      ``CurrentBeetsAmbiguous``: every Ambiguous reason
      (``multiple_matches``/``conflicting_identity``/``empty_topology``/
      ``invalid_path``/``unresolved_relative_path``/``split_topology``)
      requires at least one matching album row, so the id is STILL held,
      never gone.
    * "changed occupant" is true ONLY when ``old_after`` is
      ``CurrentBeetsUnique`` at a DIFFERENT ``album_id`` than
      ``old_before`` named — never when nothing changed, and never when
      the id is ambiguous or missing instead.
    * A "did not move" detail may NEVER also claim library-wide stasis
      (round 3 F-4): a CONCURRENT writer can move ``new_identity``
      independently of this execution's row (``old_after`` unchanged,
      ``new_after`` differing from ``new_before``) — the production
      wording was corrected to scope its subject to "the row this
      execution targeted", never "the library", but this clause still
      inspects ``new_before``/``new_after`` so it can catch a regression
      back to the retired, self-contradicting "library"-scoped wording:
      that phrase paired with a genuinely moved ``new_after`` is exactly
      the shipped self-contradiction.
    """
    if outcome != RETAG_FAILED:
        return
    old_before_id = (
        old_before.album_id if isinstance(old_before, CurrentBeetsUnique) else None
    )
    if "did not move" in detail:
        unchanged = (
            isinstance(old_after, CurrentBeetsUnique)
            and old_before_id is not None
            and old_after.album_id == old_before_id
        )
        if not unchanged:
            raise AssertionError(
                "detail claims the library did not move, but old_after "
                f"({old_after!r}) is not the same album old_before "
                f"({old_before!r}) named"
            )
        if "library" in detail and new_before != new_after:
            raise AssertionError(
                "detail claims the WHOLE LIBRARY did not move, but "
                f"new_after ({new_after!r}) differs from new_before "
                f"({new_before!r}) — a \"did not move\" claim may only "
                "ever be scoped to the row this execution targeted"
            )
    if "moved off" in detail:
        old_is_gone = isinstance(old_after, CurrentBeetsMissing)
        if not old_is_gone:
            raise AssertionError(
                "detail claims the library moved off the old id, but "
                f"old_after is {old_after!r}, not CurrentBeetsMissing "
                "(still held)"
            )
    if "changed occupant" in detail:
        different_occupant = (
            isinstance(old_after, CurrentBeetsUnique)
            and old_before_id is not None
            and old_after.album_id != old_before_id
        )
        if not different_occupant:
            raise AssertionError(
                "detail claims the old id changed occupant, but old_after "
                f"({old_after!r}) is not a different album than old_before "
                f"({old_before!r})"
            )


def check_query_and_guard_agree_on_storage_shape(
    *,
    guard_matches: bool,
    query_matches: bool,
    shape: MbAlbumidStorageShape,
    identity_case: str,
) -> None:
    """M1 (#1093 item 2, review F2 + round 3 F1) — the retag query's
    compiled SQL clause and the post-retag guard's own matching SQL
    (``lib.beets_db.BeetsDB.resolve_current_release`` — the exact method
    ``retag_merged_album`` re-reads with) must select the SAME row for
    every ``mb_albumid`` storage shape a raw third-party writer could
    produce, and for every case the QUERIED identity itself could take.
    ``guard_matches``/``query_matches`` are each computed by an
    INDEPENDENT SQL execution (see ``TestQueryAndGuardConvergeOnStorageShape``)
    — this checker only compares the two booleans, so it cannot pass "by
    construction" the way a checker that recomputes one side from the
    other can (`.claude/rules/code-quality.md` § "Agree by construction").
    """
    if guard_matches != query_matches:
        raise AssertionError(
            f"guard and query DISAGREE on storage shape {shape.label!r} "
            f"({shape.value!r}) with queried identity_case={identity_case!r}: "
            f"guard_matches={guard_matches}, query_matches={query_matches}"
        )


# ---------------------------------------------------------------------------
# Strategies over the world space — no plausibility filters
# ---------------------------------------------------------------------------

#: Album-id cardinalities the fake resolver turns into missing / unique /
#: ambiguous, exactly as the real resolver does.
CARDINALITIES = st.sampled_from([(), (7,), (7, 8)])

#: What one ``beet modify`` invocation does. ``returncode`` is present
#: precisely because it must not decide anything: a query matching nothing
#: exits 1 (``UserError``), but a query that MATCHES and produces no field
#: change still prints "No changes to make." and exits 0 — and either way,
#: an exit code read against a shared SQLite file another process can
#: concurrently mutate is not itself evidence of the end state.
MODIFY_RESULTS = st.sampled_from(["exit_0", "exit_1", "raises_timeout", "raises_oserror"])

#: What the library looks like AFTER modify ran — including the worlds
#: where it did nothing, moved cleanly, moved halfway, or invented a second
#: album.
POST_STATES = st.sampled_from([
    "unchanged",
    "moved",
    "moved_ambiguous",
    "old_gone_new_absent",
    "both_present",
    "old_ambiguous",
    "old_displaced",
])


def _apply_post_state(beets: FakeBeetsDB, post_state: str) -> None:
    if post_state == "unchanged":
        return
    if post_state == "moved":
        beets.set_album_ids_for_release(MERGED, [])
        beets.set_album_ids_for_release(SURVIVOR, [7])
        return
    if post_state == "moved_ambiguous":
        beets.set_album_ids_for_release(MERGED, [])
        beets.set_album_ids_for_release(SURVIVOR, [7, 8])
        return
    if post_state == "old_gone_new_absent":
        beets.set_album_ids_for_release(MERGED, [])
        beets.set_album_ids_for_release(SURVIVOR, [])
        return
    if post_state == "both_present":
        beets.set_album_ids_for_release(MERGED, [7])
        beets.set_album_ids_for_release(SURVIVOR, [8])
        return
    if post_state == "old_displaced":
        # #1093 review round 2 sub-point — a DIFFERENT single album (9, not
        # the original 7) now occupies the old id: the "changed occupant"
        # shape, distinct from both "did not move" (still 7) and "moved
        # off" (empty).
        beets.set_album_ids_for_release(MERGED, [9])
        return
    beets.set_album_ids_for_release(MERGED, [7, 8])


def _modify(
    result: str,
    apply_post_state: Callable[[], None],
    calls: list[tuple[tuple[str, str], str]],
) -> Callable[[tuple[str, str], str], BeetsChildRun]:
    def run(query_tokens: tuple[str, str], assignment: str) -> BeetsChildRun:
        calls.append((query_tokens, assignment))
        apply_post_state()
        if result == "raises_timeout":
            raise sp.TimeoutExpired(cmd=["beets", "modify"], timeout=120)
        if result == "raises_oserror":
            raise OSError("No such file or directory: beets python")
        return BeetsChildRun(
            returncode=0 if result == "exit_0" else 1, stdout="", stderr="",
        )

    return run


def _snapshot(
    beets: FakeBeetsDB,
) -> tuple[CurrentBeetsResolution, CurrentBeetsResolution]:
    resolutions = beets.resolve_current_releases([OLD, NEW])
    return resolutions[OLD], resolutions[NEW]


class TestRetagProperties(unittest.TestCase):
    """G1–G5 over every world, driving the real retag against the real fake."""

    @settings(deadline=None)
    @given(
        old_ids=CARDINALITIES,
        new_ids=CARDINALITIES,
        modify_result=MODIFY_RESULTS,
        post_state=POST_STATES,
    )
    # The decisive worlds: a clean retag, and the one that motivated the
    # whole "exit status is not evidence" contract — modify exits 0 while
    # the library never moved.
    @example(
        old_ids=(7,), new_ids=(), modify_result="exit_0", post_state="moved",
    )
    @example(
        old_ids=(7,), new_ids=(), modify_result="exit_0", post_state="unchanged",
    )
    @example(
        old_ids=(7,), new_ids=(8,), modify_result="exit_0", post_state="moved",
    )
    # #1093 item 5 — the world that produced the self-contradictory
    # "did not move" detail: a partial move where the old id genuinely
    # moves away but the survivor lands ambiguous across two albums.
    @example(
        old_ids=(7,), new_ids=(), modify_result="exit_0",
        post_state="moved_ambiguous",
    )
    # #1093 review round 2 (F1) — the reviewer's own counterexample: a
    # concurrent writer lands a second album at the old id while modify
    # moves nothing; old_after is Ambiguous, never "moved off".
    @example(
        old_ids=(7,), new_ids=(), modify_result="exit_1",
        post_state="old_ambiguous",
    )
    # #1093 review round 2 sub-point — a DIFFERENT album occupies the old
    # id afterward: neither "did not move" nor "moved off" is true.
    @example(
        old_ids=(7,), new_ids=(), modify_result="exit_0",
        post_state="old_displaced",
    )
    def test_every_world_upholds_the_retag_invariants(
        self,
        old_ids: tuple[int, ...],
        new_ids: tuple[int, ...],
        modify_result: str,
        post_state: str,
    ) -> None:
        beets = library(old_album_ids=old_ids, new_album_ids=new_ids)
        old_before, new_before = _snapshot(beets)
        calls: list[tuple[tuple[str, str], str]] = []

        with _silence_logs():
            result = retag_merged_album(
                beets,
                old_identity=OLD,
                new_identity=NEW,
                run_modify=_modify(
                    modify_result, lambda: _apply_post_state(beets, post_state),
                    calls,
                ),
            )

        old_after, new_after = _snapshot(beets)
        check_ready_only_when_rekeyable(
            result.outcome, old_after=old_after, new_after=new_after,
        )
        check_modify_only_for_a_uniquely_held_old_id(
            calls, old_before=old_before, new_before=new_before,
        )
        check_query_and_assignment_name_the_right_identity(
            calls, old_identity=OLD, new_identity=NEW, old_before=old_before,
        )
        check_both_held_is_never_ready(
            result.outcome, old_before=old_before, new_before=new_before,
        )
        check_failure_detail_does_not_contradict_the_observed_move(
            result.outcome, result.detail,
            old_before=old_before, old_after=old_after,
            new_before=new_before, new_after=new_after,
        )
        self.assertTrue(result.detail, "every outcome carries a diagnostic")

    @settings(deadline=None)
    @given(
        old_ids=CARDINALITIES,
        new_ids=CARDINALITIES,
        modify_result=MODIFY_RESULTS,
        post_state=POST_STATES,
        fail_on_snapshot=st.sampled_from([1, 2]),
    )
    def test_an_unreadable_authority_never_authorizes_a_rekey(
        self,
        old_ids: tuple[int, ...],
        new_ids: tuple[int, ...],
        modify_result: str,
        post_state: str,
        fail_on_snapshot: int,
    ) -> None:
        """An unreadable Beets authority is a failure, never absence.

        Reading a failed authority read as "the album is not held" would
        return ``not_held`` — a READY outcome — and rekey a request whose
        library state nobody actually observed. The failure is injected at
        the pre-retag snapshot and at the post-retag re-read in turn; when
        the retag path is never taken the second snapshot never happens, so
        the assertion is keyed on the read having actually failed.
        """
        inner = library(old_album_ids=old_ids, new_album_ids=new_ids)

        class Unreadable:
            def __init__(self) -> None:
                self.snapshots = 0
                self.raised = False

            def resolve_current_releases(
                self, identities: list[ReleaseIdentity],
            ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
                self.snapshots += 1
                if self.snapshots >= fail_on_snapshot:
                    self.raised = True
                    raise sqlite3.OperationalError("database is locked")
                return inner.resolve_current_releases(identities)

        resolver = Unreadable()
        calls: list[tuple[tuple[str, str], str]] = []
        with _silence_logs():
            result = retag_merged_album(
                resolver,
                old_identity=OLD,
                new_identity=NEW,
                run_modify=_modify(
                    modify_result, lambda: _apply_post_state(inner, post_state),
                    calls,
                ),
            )

        if resolver.raised:
            self.assertEqual(result.outcome, RETAG_FAILED)
            self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        else:
            # The retag path was never taken, so only one snapshot happened
            # and every answer is backed by a real observation.
            self.assertNotEqual(result.outcome, RETAG_RETAGGED)


# ---------------------------------------------------------------------------
# M1 — the query and the guard converge on every REAL mb_albumid storage
# shape, proven by two independently executed SQL statements (#1093 item 2
# review F2)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MbAlbumidStorageShape:
    label: str
    #: The raw value written into the `mb_albumid` column via a raw
    #: third-party SQL write (never through Beets' own ORM, which always
    #: writes `str`). `bytes` produces a genuine SQLite BLOB storage class;
    #: `None` produces NULL; `str` produces plain TEXT.
    value: bytes | str | None


#: Every storage shape a raw third-party writer could put in `mb_albumid`:
#: the exact target value, an unrelated value, case/whitespace variants, a
#: same-prefix decoy, NULL, and BLOB-encoded bytes (both matching and not).
MB_ALBUMID_STORAGE_SHAPES: tuple[MbAlbumidStorageShape, ...] = (
    MbAlbumidStorageShape("exact_text", MERGED),
    MbAlbumidStorageShape("different_text", SURVIVOR),
    MbAlbumidStorageShape("case_upper", MERGED.upper()),
    MbAlbumidStorageShape("whitespace_trailing", MERGED + " "),
    MbAlbumidStorageShape("whitespace_leading", " " + MERGED),
    MbAlbumidStorageShape("whitespace_newline", MERGED + "\n"),
    MbAlbumidStorageShape("prefix_extended", MERGED + "0"),
    MbAlbumidStorageShape("prefix_truncated", MERGED[:-1]),
    MbAlbumidStorageShape("blob_exact", MERGED.encode("utf-8")),
    MbAlbumidStorageShape("blob_different", SURVIVOR.encode("utf-8")),
    MbAlbumidStorageShape("null", None),
)

MB_ALBUMID_STORAGE_SHAPE_STRATEGY = st.sampled_from(MB_ALBUMID_STORAGE_SHAPES)

def _queried_identity(case: str) -> ReleaseIdentity:
    """The QUERIED identity's own case.

    ``case="exact"`` is the only case reachable in production: every
    ``ReleaseIdentity`` that reaches
    :func:`lib.beets_retag.retag_merged_album` is built via
    ``ReleaseIdentity.from_id``, which lowercases UUIDs.

    ``case="upper"`` is still drawn by the generated property and the
    #1138 pin — production cannot construct it, but
    ``resolve_current_releases`` must attribute a SQL-fetched row anyway.
    """
    release_id = MERGED if case == "exact" else MERGED.upper()
    return ReleaseIdentity(source="musicbrainz", release_id=release_id)


@cache
def _mb_albumid_convergence_world() -> tuple[Path, int, Path]:
    """One real beets-schema SQLite file (module-cached, built ONCE): a
    single album row whose ``mb_albumid`` generated examples overwrite via
    raw SQL, then read back through two independent mechanisms. Real beets
    schema (via a real ``beets.library.Library``), not a hand-derived
    approximation — so a future schema change is reflected here too. The
    library root is returned too: the guard side
    (``BeetsDB.resolve_current_release``) resolves item paths against it
    (#1093 review round 3, F1)."""
    tmp = tempfile.mkdtemp(prefix="cratedigger_mb_albumid_convergence_")
    root = Path(tmp) / "library"
    root.mkdir()
    album_dir = root / "Convergence Artist" / "1999 - Album"
    album_dir.mkdir(parents=True)
    track_path = album_dir / "01 Track.mp3"
    track_path.write_bytes(b"placeholder")
    library_db = Path(tmp) / "library.db"
    lib = beets_library.Library(str(library_db), str(root))
    item = beets_library.Item(
        path=str(track_path), title="Track", artist="Convergence Artist",
        album="Album", albumartist="Convergence Artist", track=1, disc=1,
        year=1999, mb_albumid=MERGED,
        mb_trackid="00000000-1111-4111-8111-111111111111",
    )
    album = lib.add_album([item])
    if album.id is None:
        raise AssertionError("seeded Beets album is missing its database id")
    album_id = album.id
    lib._close()
    return library_db, album_id, root


def _write_mb_albumid(
    library_db: Path, album_id: int, value: bytes | str | None,
) -> None:
    """A raw third-party SQL write — the shape a writer OTHER than Beets
    itself (which always writes ``str``) could produce."""
    conn = sqlite3.connect(str(library_db))
    conn.execute(
        "UPDATE albums SET mb_albumid = ? WHERE id = ?", (value, album_id),
    )
    conn.commit()
    conn.close()


def _guard_matches(
    library_db: Path, library_root: Path, identity: ReleaseIdentity,
) -> bool:
    """The REAL guard — ``lib.beets_db.BeetsDB.resolve_current_release`` —
    the EXACT method :func:`lib.beets_retag.retag_merged_album` re-reads
    the library with both before AND after ``beet modify`` runs. NOT
    ``_matching_album_ids``: that method has exactly one production caller
    (``get_all_album_ids_for_release``, consumed only by
    ``harness/import_one.py``'s post-import stale cleanup) and is a
    bystander to the retag guard entirely — a mutation to
    ``resolve_current_releases``' own SQL (verified live: a real
    case-insensitivity mutant, `LOWER(...)` on both the SQL comparison and
    the Python-side re-key, made the guard match a row this query does
    not) left the bystander-driven version of this property green on all
    11 examples — one per entry in ``MB_ALBUMID_STORAGE_SHAPES`` — (#1093
    review round 3, F1). A fresh read-only connection per call, mirroring
    how production reopens it."""
    with BeetsDB(str(library_db), library_root=str(library_root)) as beets:
        resolution = beets.resolve_current_release(identity)
    return not isinstance(resolution, CurrentBeetsMissing)


def _query_matches(library_db: Path, album_id: int, identity: ReleaseIdentity) -> bool:
    """The REAL retag query's compiled clause — parsed via the real Beets
    query parser, executed directly against the SAME data on a connection
    carrying Beets' own UDFs (via a real, throwaway ``Library``, never a
    hand-copied ``regexp()`` re-implementation), so a reverted-to-regex
    mutant is evaluated faithfully rather than merely erroring on a missing
    SQL function."""
    id_token, mb_albumid_token = retag_album_query(identity, album_id=album_id)
    query, _sort = parse_query_parts(
        [id_token, mb_albumid_token], beets_library.Album,
    )
    assert isinstance(query, CollectionQuery)  # narrow for pyright
    sql, params = query.clause()
    assert sql is not None
    conn = sqlite3.connect(str(library_db))
    throwaway_lib = beets_library.Library(str(library_db))
    throwaway_lib.add_functions(conn)
    throwaway_lib._close()
    rows = conn.execute(f"SELECT id FROM albums WHERE {sql}", params).fetchall()
    conn.close()
    matched_ids = {int(row[0]) for row in rows}
    return album_id in matched_ids


class TestQueryAndGuardConvergeOnStorageShape(unittest.TestCase):
    """M1 (#1093 item 2, review F2 + round 3 F1) — the retag query's
    compiled clause and the guard's own matching SQL
    (``BeetsDB.resolve_current_release``, the exact method
    ``retag_merged_album`` re-reads with — NOT the unrelated
    ``_matching_album_ids``, whose only production caller is
    ``harness/import_one.py``'s post-import stale cleanup) agree on EVERY
    generated ``mb_albumid`` storage shape, crossed against every case the
    QUERIED identity itself could take, computed via two INDEPENDENT SQL
    executions against the SAME real beets-schema row — never by
    recomputing one mechanism and comparing it to itself.

    This is the real property G3 cannot be: G3 (in
    ``check_query_and_assignment_name_the_right_identity``) recomputes
    :func:`retag_album_query` on both sides of its comparison, which
    proves the CALLER passed the builder's real output through unmodified
    but is structurally incapable of catching a change to what
    :func:`retag_album_query` itself selects — reverting it to the retired
    anchored-regex form still passes G3 (both sides call the same mutated
    function), but fails THIS property immediately, because
    ``_query_matches`` parses and executes the query's own compiled SQL
    independently of ``_guard_matches``. Live-verified the same way in the
    other direction: a real mutant making ``resolve_current_releases``
    case-insensitive (`LOWER(...)` on the SQL comparison AND the Python-side
    re-key) is invisible to a guard side driven by ``_matching_album_ids``
    (all 11 examples — one per ``MB_ALBUMID_STORAGE_SHAPES`` entry —
    passed) but is caught immediately once the guard side drives the real
    ``resolve_current_release``.
    """

    @settings(deadline=None)
    @given(
        shape=MB_ALBUMID_STORAGE_SHAPE_STRATEGY,
        identity_case=st.sampled_from(("exact", "upper")),
    )
    def test_query_and_guard_agree_on_every_storage_shape(
        self, shape: MbAlbumidStorageShape, identity_case: str,
    ) -> None:
        library_db, album_id, library_root = _mb_albumid_convergence_world()
        _write_mb_albumid(library_db, album_id, shape.value)
        identity = _queried_identity(identity_case)

        guard_matches = _guard_matches(library_db, library_root, identity)
        query_matches = _query_matches(library_db, album_id, identity)

        check_query_and_guard_agree_on_storage_shape(
            guard_matches=guard_matches,
            query_matches=query_matches,
            shape=shape,
            identity_case=identity_case,
        )


class TestPreviouslyDivergentQueriedIdentityCaseConverges(unittest.TestCase):
    """#1138 — the one 11×2 cell that used to disagree now agrees.

    Queried identity is UPPER (unreachable via ``ReleaseIdentity.from_id``).
    Stored ``mb_albumid`` is the same uppercase text. SQL fetches the row;
    the Python re-key must attribute it. The retag ``MatchQuery`` already
    matched. Both sides must report a hit.
    """

    def test_matching_non_normalized_identity_converges(self) -> None:
        shape = next(
            s for s in MB_ALBUMID_STORAGE_SHAPES if s.label == "case_upper"
        )
        library_db, album_id, library_root = _mb_albumid_convergence_world()
        _write_mb_albumid(library_db, album_id, shape.value)
        identity = _queried_identity("upper")

        guard_matches = _guard_matches(library_db, library_root, identity)
        query_matches = _query_matches(library_db, album_id, identity)

        self.assertTrue(
            guard_matches,
            "SQL fetched the uppercase row; Python attribution must "
            "attach it to the queried identity",
        )
        self.assertTrue(
            query_matches,
            "the retag query's MatchQuery does a single exact SQL "
            "comparison, so it matches the same-case stored value",
        )


class TestRealModifyRetagOverItemCountBoundaries(unittest.TestCase):
    """Deterministic pins (#1087 review, F3) for the item-count equivalence
    classes T6/T7 rest on — NOT a certified generated domain.

    This surface was previously wrapped in ``finite_generated_domain``, but
    that decorator's contract is an INDEPENDENT enumeration of the semantic
    world space (its other two users reconstruct
    ``itertools.product((True, False), repeat=4)`` or enumerate every mask —
    see ``tests/test_web_auth_mode_generated.py::verify_mode_world_domain``
    and ``tests/test_preview_manifest_generated.py::
    verify_extra_filename_mask_domain``). ``ITEM_COUNTS`` here
    is a hand-reasoned equivalence-class argument (0 = the empty-topology
    fail-closed branch, verified for real against ``BeetsDB``'s
    ``LEFT JOIN`` authority read; 1 = the smallest world where ``beet
    modify`` actually runs; 2 = the smallest "many" world, proving this
    module's own per-item checks iterate every item rather than passing on
    an index-0-only check a singleton could not distinguish from correct —
    every count above 2 repeats that identical per-item code path with no
    cross-item interaction), recorded beside the constant in
    ``tests/test_beets_retag.py``. That argument is substantively sound, but
    it is a REASONED PARTITION, not a proof a checker can independently
    verify the way it can reconstruct a boolean product — so it is pinned
    honestly as three deterministic cases, not claimed as "certified".

    The generated-property half of the T1–T5 pair already lives in
    ``TestRetagProperties`` above, which drives the REAL
    ``retag_merged_album`` over the genuinely combinatorial (cardinality ×
    modify-result × post-state) FakeBeetsDB world space. T6/T7's real
    subprocess coverage does not need a matching generated property: the
    reviewer independently confirmed ``Album.store``'s per-item loop has no
    cross-item interaction, so there is no richer world space beyond item
    count for a property to explore.

    Each case runs the REAL composed ``retag_merged_album`` once (memoised
    by ``observe_real_modify_retag``).
    """

    def test_every_item_count_boundary_behaves_correctly(self) -> None:
        for item_count in ITEM_COUNTS:
            with self.subTest(item_count=item_count):
                observation = observe_real_modify_retag(item_count)
                if item_count == 0:
                    # T7 — a real, reachable empty-item album fails closed
                    # BEFORE `beet modify` ever runs; it is not the same
                    # outcome class as every other count, and asserting
                    # `moved_every_identity` here would be asserting a
                    # world the composition cannot produce.
                    check_real_modify_retag_refuses_empty_topology(observation)
                else:
                    check_real_modify_retag_moved_every_identity(observation)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Every checker owes a planted violation proving it can fail."""

    def _unique(
        self, identity: ReleaseIdentity, *, album_id: int = 7,
    ) -> CurrentBeetsUnique:
        return CurrentBeetsUnique(
            identity=identity,
            album_id=album_id,
            album_path=f"/library/album-{album_id}",
            items=(),
            selectors=(f"mb_albumid:{identity.release_id}",),
        )

    def _missing(self, identity: ReleaseIdentity) -> CurrentBeetsMissing:
        return CurrentBeetsMissing(identity=identity)

    def _ambiguous(self, identity: ReleaseIdentity) -> CurrentBeetsAmbiguous:
        return CurrentBeetsAmbiguous(
            identity=identity, album_ids=(7, 8), reason="multiple_matches",
        )

    def test_ready_while_the_old_id_is_still_held_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_ready_only_when_rekeyable(
                RETAG_RETAGGED,
                old_after=self._unique(OLD),
                new_after=self._unique(NEW),
            )

    def test_ready_while_the_new_id_is_ambiguous_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_ready_only_when_rekeyable(
                RETAG_RETAGGED,
                old_after=self._missing(OLD),
                new_after=self._ambiguous(NEW),
            )

    def test_modify_on_a_missing_old_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, r"only a uniquely held old album",
        ):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
                old_before=self._missing(OLD),
                new_before=self._missing(NEW),
            )

    def test_modify_while_the_survivor_is_already_held_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            r"collide two albums under one duplicate key",
        ):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
            )

    def test_modify_invoked_twice_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            r"beet modify was invoked 2 times for one album",
        ):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))] * 2,
                old_before=self._unique(OLD),
                new_before=self._missing(NEW),
            )

    def test_a_query_naming_the_survivor_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, r"modify query names the survivor",
        ):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(NEW, album_id=7), retag_assignment(NEW))],
                old_identity=OLD, new_identity=NEW, old_before=self._unique(OLD),
            )

    def test_a_query_using_a_different_mechanism_is_rejected(self) -> None:
        """#1093 (round-3-of-round-3 N-4 correction) — the invariant this
        self-test proves ("the query can only ever name albums filed
        under exactly the old id") is now enforced through the
        exact-match mechanism, not a different one; this world names the
        old id's value token with a single ``:`` (real beets:
        ``field:value`` with no further prefix falls to the field's
        default query class, ``SubstringQuery`` for ``mb_albumid`` — NOT
        a regex; that requires the DOUBLE colon ``field::value``, which is
        what #1093 actually retired, ``mb_albumid::^<id>\\Z``) instead of
        the exact-match ``:=`` token the module actually emits, and the
        checker must still catch the mismatch even with a correct id
        token alongside it."""
        with self.assertRaisesRegex(
            AssertionError,
            r"is not the compound exact-match query for",
        ):
            check_query_and_assignment_name_the_right_identity(
                [(("id:=7", f"mb_albumid:{MERGED}"), retag_assignment(NEW))],
                old_identity=OLD, new_identity=NEW, old_before=self._unique(OLD),
            )

    def test_an_assignment_naming_the_merged_away_id_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, r"names the merged-away id",
        ):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(OLD, album_id=7), retag_assignment(OLD))],
                old_identity=OLD, new_identity=NEW, old_before=self._unique(OLD),
            )

    def test_a_syntactically_valid_but_wrong_assignment_is_rejected(
        self,
    ) -> None:
        """#1093 round-3 review F-2 — the checker's FINAL clause
        (``assignment != retag_assignment(new_identity)``) was unreached by
        every existing self-test: the only prior "wrong assignment" world
        (``test_an_assignment_naming_the_merged_away_id_is_rejected``, just
        above) uses ``retag_assignment(OLD)``, whose value literally
        contains ``old_identity.release_id`` — so it always trips the
        EARLIER "names the merged-away id" clause first, never this one.
        This world names neither the old id nor the exact new-identity
        assignment (an uppercased variant of the real
        ``retag_assignment(NEW)`` value — same shape, wrong case, so it
        equality-compares false without containing either id string) —
        every earlier clause passes, and only the final clause can fire.
        """
        wrong_assignment = retag_assignment(NEW).upper()
        self.assertNotIn(OLD.release_id, wrong_assignment)
        self.assertNotEqual(wrong_assignment, retag_assignment(NEW))

        with self.assertRaisesRegex(
            AssertionError, r"is not the survivor assignment for",
        ):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(OLD, album_id=7), wrong_assignment)],
                old_identity=OLD, new_identity=NEW, old_before=self._unique(OLD),
            )

    def test_modify_invoked_while_the_old_id_became_ambiguous_is_rejected(
        self,
    ) -> None:
        """G3's own id-pin sub-clause (#1093 review residual): the query
        must be pinned to the OLD id's resolved album_id, which only
        exists when old_before is uniquely held. An Ambiguous old_before
        reaching this checker (a world G2 alone would not catch, since G2
        governs whether modify SHOULD have run, not what it was pinned to)
        is rejected outright — there is no album_id to pin the query to."""
        with self.assertRaisesRegex(AssertionError, "without a uniquely-held"):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
                old_identity=OLD, new_identity=NEW, old_before=self._ambiguous(OLD),
            )

    def test_a_ready_outcome_on_the_double_sided_merge_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_both_held_is_never_ready(
                RETAG_RETAGGED,
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
            )

    def test_a_did_not_move_claim_while_the_old_id_is_missing_is_rejected(
        self,
    ) -> None:
        """G5 (#1093 item 5, round 1) — the exact contradiction: the old id
        moved away (Missing), so a detail claiming "did not move" is a lie."""
        with self.assertRaisesRegex(AssertionError, "is not the same album old_before"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                "beet modify exited 0, but the row this execution targeted "
                f"did not move: {MERGED} is not held; {SURVIVOR} is "
                "ambiguous (multiple_matches) across albums 7, 8",
                old_before=self._unique(OLD), old_after=self._missing(OLD),
                new_before=self._missing(NEW), new_after=self._missing(NEW),
            )

    def test_a_did_not_move_claim_while_the_old_id_is_ambiguous_is_rejected(
        self,
    ) -> None:
        """The other non-Unique shape: the old id is now ambiguous, which
        is equally not "did not move" (it changed FROM Unique)."""
        with self.assertRaisesRegex(AssertionError, "is not the same album old_before"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                "beet modify exited 0, but the row this execution targeted "
                f"did not move: {MERGED} is ambiguous (multiple_matches) "
                f"across albums 7, 8; {SURVIVOR} is not held",
                old_before=self._unique(OLD), old_after=self._ambiguous(OLD),
                new_before=self._missing(NEW), new_after=self._missing(NEW),
            )

    def test_a_did_not_move_claim_while_a_different_album_now_occupies_it_is_rejected(
        self,
    ) -> None:
        """G5 (#1093 review round 2 sub-point) — old_after IS still Unique,
        but at a DIFFERENT album than old_before named: the original
        occupant is gone, a different album took the id. Still not "did
        not move"."""
        with self.assertRaisesRegex(AssertionError, "is not the same album old_before"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                "beet modify exited 0, but the row this execution targeted "
                f"did not move: {MERGED} is uniquely held as album 9; "
                f"{SURVIVOR} is not held",
                old_before=self._unique(OLD, album_id=7),
                old_after=self._unique(OLD, album_id=9),
                new_before=self._missing(NEW), new_after=self._missing(NEW),
            )

    def test_a_did_not_move_claim_while_the_old_id_is_still_unique_passes(
        self,
    ) -> None:
        """Must-still-work: the ONE world where "did not move" is true —
        the checker must not reject a truthful detail."""
        check_failure_detail_does_not_contradict_the_observed_move(
            RETAG_FAILED,
            "beet modify exited 0, but the row this execution targeted did "
            f"not move: {MERGED} is uniquely held as album 7; {SURVIVOR} is "
            "not held",
            old_before=self._unique(OLD, album_id=7),
            old_after=self._unique(OLD, album_id=7),
            new_before=self._missing(NEW), new_after=self._missing(NEW),
        )

    def test_a_did_not_move_claim_scoped_to_the_library_while_the_survivor_moved_is_rejected(
        self,
    ) -> None:
        """#1093 round 3 review F-4 — the ADDED clause: a detail using the
        RETIRED, over-broad "the library did not move" wording is rejected
        the moment new_after shows the survivor genuinely moved
        (Missing before, uniquely held afterward) — even though old_after
        is unchanged and would otherwise satisfy the existing clause. This
        is the exact self-contradiction the production wording was
        corrected to stop making (production now says "the row this
        execution targeted", never "the library" — see the sibling
        "passes" test above); this self-test proves the checker itself
        would still catch a regression back to the retired phrasing."""
        with self.assertRaisesRegex(AssertionError, "WHOLE LIBRARY"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                "beet modify exited 0, but the library did not move: "
                f"{MERGED} is uniquely held as album 7; {SURVIVOR} is "
                "uniquely held as album 8",
                old_before=self._unique(OLD, album_id=7),
                old_after=self._unique(OLD, album_id=7),
                new_before=self._missing(NEW),
                new_after=self._unique(NEW, album_id=8),
            )

    def test_a_moved_off_claim_while_the_old_id_is_ambiguous_is_rejected(
        self,
    ) -> None:
        """G5's converse clause (#1093 review round 2, F1) — "moved off"
        must imply the old id is actually GONE. Ambiguous means the
        opposite: at least one album row still matches it."""
        with self.assertRaisesRegex(AssertionError, "moved off"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                "beet modify exited 0; the library moved off "
                f"{MERGED} but did not land at a state the caller may "
                f"rekey onto: {MERGED} is now ambiguous (multiple_matches) "
                f"across albums 7, 9; {SURVIVOR} is not held",
                old_before=self._unique(OLD, album_id=7),
                old_after=self._ambiguous(OLD),
                new_before=self._missing(NEW), new_after=self._missing(NEW),
            )

    def test_a_moved_off_claim_while_the_old_id_is_missing_passes(self) -> None:
        """Must-still-work: "moved off" is true exactly when old_after is
        Missing."""
        check_failure_detail_does_not_contradict_the_observed_move(
            RETAG_FAILED,
            "beet modify exited 0; the library moved off "
            f"{MERGED} but did not land at a state the caller may rekey "
            f"onto: {MERGED} is now not held; {SURVIVOR} is not held",
            old_before=self._unique(OLD, album_id=7),
            old_after=self._missing(OLD),
            new_before=self._missing(NEW), new_after=self._missing(NEW),
        )

    def test_a_changed_occupant_claim_while_the_album_is_unchanged_is_rejected(
        self,
    ) -> None:
        """G5's third clause (#1093 review round 2) — "changed occupant"
        must imply a DIFFERENT album than old_before named; claiming it
        while nothing actually changed is equally a lie."""
        with self.assertRaisesRegex(AssertionError, "changed occupant"):
            check_failure_detail_does_not_contradict_the_observed_move(
                RETAG_FAILED,
                f"beet modify exited 0; {MERGED} changed occupant: was "
                f"album 7, is now uniquely held as album 7; {SURVIVOR} is "
                "not held",
                old_before=self._unique(OLD, album_id=7),
                old_after=self._unique(OLD, album_id=7),
                new_before=self._missing(NEW), new_after=self._missing(NEW),
            )

    def test_a_changed_occupant_claim_for_a_genuinely_different_album_passes(
        self,
    ) -> None:
        """Must-still-work: "changed occupant" is true exactly when
        old_after is Unique at a different album_id than old_before."""
        check_failure_detail_does_not_contradict_the_observed_move(
            RETAG_FAILED,
            f"beet modify exited 0; {MERGED} changed occupant: was album "
            f"7, is now uniquely held as album 9; {SURVIVOR} is not held",
            old_before=self._unique(OLD, album_id=7),
            old_after=self._unique(OLD, album_id=9),
            new_before=self._missing(NEW), new_after=self._missing(NEW),
        )

    def test_a_non_failed_outcome_is_not_checked(self) -> None:
        """The clause only governs FAILED details — a retagged/ambiguous
        outcome's detail is out of scope regardless of its wording."""
        check_failure_detail_does_not_contradict_the_observed_move(
            RETAG_RETAGGED, "the library did not move",
            old_before=self._unique(OLD), old_after=self._missing(OLD),
            new_before=self._missing(NEW),
            new_after=self._unique(NEW, album_id=8),
        )

    @staticmethod
    def _real_observation(
        *,
        item_dir: str = "/library/Installed Artist/1999 - Installed Album",
        entries: tuple[str, ...] = (
            "01 Installed 1.mp3", "02 Installed 2.mp3", SIDECAR_NAME,
        ),
        album_mb_albumid: str = SURVIVOR,
        item_mb_albumids: tuple[str, ...] = (SURVIVOR, SURVIVOR),
        outcome: RetagOutcome = RETAG_RETAGGED,
        item_count: int = 2,
        item_mtimes_before_ns: tuple[int, ...] = (1_000, 2_000),
        item_mtimes_after_ns: tuple[int, ...] = (1_000, 2_000),
    ) -> RealModifyObservation:
        """One real-primitive observation, defaulting to the legitimate retag."""
        return RealModifyObservation(
            item_count=item_count,
            variant="planted",
            result=BeetsRetagResult(outcome=outcome, detail="planted"),
            album_mb_albumid=album_mb_albumid,
            item_mb_albumids=item_mb_albumids,
            item_paths=(
                f"{item_dir}/01 Installed 1.mp3",
                f"{item_dir}/02 Installed 2.mp3",
            ),
            installed_dir_entries=tuple(sorted(entries)),
            item_mtimes_before_ns=item_mtimes_before_ns,
            item_mtimes_after_ns=item_mtimes_after_ns,
        )

    def test_a_relocated_file_is_rejected(self) -> None:
        """The exact ``-M``-less mutant shape: identity moved, files moved
        too."""
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(self._real_observation(
                item_dir="/library/New Artist/2001 - New Album",
                entries=(),
            ))
        self.assertIn("RELOCATED", str(caught.exception))

    def test_a_pruned_sidecar_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError, "sidecar is gone from the album directory",
        ):
            check_real_modify_retag_moved_every_identity(self._real_observation(
                entries=("01 Installed 1.mp3", "02 Installed 2.mp3"),
            ))

    def test_an_album_that_never_moved_is_rejected(self) -> None:
        """The exact ``-a``-less mutant: items moved, the album row did not."""
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(self._real_observation(
                album_mb_albumid=MERGED, outcome=RETAG_FAILED,
            ))
        self.assertIn("did not retag", str(caught.exception))

    def test_an_item_that_never_moved_is_rejected(self) -> None:
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(self._real_observation(
                item_mb_albumids=(SURVIVOR, MERGED),
            ))
        self.assertIn("not every ITEM", str(caught.exception))

    def test_a_written_file_is_rejected(self) -> None:
        """F2's self-test: the -W-dropped mutant shape, planted directly —
        proves the must-still-work mtime check can fail, not only pass."""
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(self._real_observation(
                item_mtimes_before_ns=(1_000, 2_000),
                item_mtimes_after_ns=(1_000, 9_999),
            ))
        self.assertIn("WROTE", str(caught.exception))

    def test_an_empty_topology_that_actually_retagged_is_rejected(self) -> None:
        """T7's self-test: a would-be world where the empty-item album
        somehow reports ``retagged`` — the composed guard must never reach
        this, but the checker must still be provably falsifiable."""
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_refuses_empty_topology(self._real_observation(
                album_mb_albumid=SURVIVOR, outcome=RETAG_RETAGGED,
                item_count=0, item_mb_albumids=(),
            ))
        self.assertIn("did not fail closed", str(caught.exception))

    def test_an_empty_topology_with_leftover_items_is_rejected(self) -> None:
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_refuses_empty_topology(self._real_observation(
                album_mb_albumid=MERGED, outcome=RETAG_AMBIGUOUS,
                item_count=0, item_mb_albumids=(MERGED,),
            ))
        self.assertIn("unexpectedly carries items", str(caught.exception))

    def test_a_guard_query_storage_shape_disagreement_is_rejected(self) -> None:
        """M1 (#1093 item 2, review F2) — a hand-built divergence between
        the two independently-computed booleans must trip."""
        with self.assertRaisesRegex(AssertionError, "DISAGREE"):
            check_query_and_guard_agree_on_storage_shape(
                guard_matches=True, query_matches=False,
                shape=MbAlbumidStorageShape("hand_built", MERGED),
                identity_case="exact",
            )
        with self.assertRaisesRegex(AssertionError, "DISAGREE"):
            check_query_and_guard_agree_on_storage_shape(
                guard_matches=False, query_matches=True,
                shape=MbAlbumidStorageShape("hand_built", MERGED),
                identity_case="upper",
            )

    def test_a_guard_query_storage_shape_agreement_passes(self) -> None:
        """Must-still-work: the checker accepts agreement in both
        directions (both matched, or neither did)."""
        check_query_and_guard_agree_on_storage_shape(
            guard_matches=True, query_matches=True,
            shape=MbAlbumidStorageShape("hand_built", MERGED),
            identity_case="exact",
        )
        check_query_and_guard_agree_on_storage_shape(
            guard_matches=False, query_matches=False,
            shape=MbAlbumidStorageShape("hand_built", MERGED),
            identity_case="upper",
        )

    def test_checkers_accept_the_legitimate_retag(self) -> None:
        """Must-still-work: a real successful retag passes every checker."""
        check_real_modify_retag_moved_every_identity(self._real_observation())
        check_real_modify_retag_refuses_empty_topology(self._real_observation(
            album_mb_albumid=MERGED, outcome=RETAG_AMBIGUOUS,
            item_count=0, item_mb_albumids=(),
        ))
        check_ready_only_when_rekeyable(
            RETAG_RETAGGED,
            old_after=self._missing(OLD),
            new_after=self._unique(NEW),
        )
        check_modify_only_for_a_uniquely_held_old_id(
            [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
            old_before=self._unique(OLD),
            new_before=self._missing(NEW),
        )
        check_query_and_assignment_name_the_right_identity(
            [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
            old_identity=OLD, new_identity=NEW, old_before=self._unique(OLD),
        )
        check_both_held_is_never_ready(
            RETAG_AMBIGUOUS,
            old_before=self._unique(OLD),
            new_before=self._unique(NEW),
        )
        check_failure_detail_does_not_contradict_the_observed_move(
            RETAG_FAILED,
            "beet modify exited 0, but the row this execution targeted did "
            f"not move: {MERGED} is uniquely held as album 7; {SURVIVOR} is "
            "not held",
            old_before=self._unique(OLD), old_after=self._unique(OLD),
            new_before=self._missing(NEW), new_after=self._missing(NEW),
        )


if __name__ == "__main__":
    unittest.main()
