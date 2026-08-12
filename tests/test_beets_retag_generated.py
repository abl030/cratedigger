"""Generated properties for the one-album ``beet modify`` retag (#1059/#1087).

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
G3  The query handed to ``modify`` always names the OLD identity and the
    assignment always names the NEW identity — never the other way, and
    never each other. Retagging the survivor is a no-op at best and a
    wrong-album mutation at worst.
G4  Both sides held never returns a ready outcome. Two installed albums that
    MusicBrainz now calls one release is the operator's decision.

``TestRealModifyRetagOverItemCountBoundaries`` below is NOT a fifth
generated property, and does not claim to be. #1075 DID ship a
real-subprocess test (``TestRealMbsyncMovesIdentityNotFiles``), so a real
subprocess is not what was missing; its fixture modelled a
RECORDING-PRESERVING merge, so the predecessor primitive's item-to-track
mapping matched and the merge it cannot actually follow — a RELEASE-ONLY
one — never ran. The lesson: a real subprocess is necessary, not
sufficient; it must run over a world shaped like the failure. That class is
composed with the T6/T7 deterministic pins over three hand-reasoned
item-count equivalence classes (0 / 1 / 2) — see the class docstring for
why that partition is honest as pins, not claimed as an independently
certified generated domain. Every genuinely combinatorial property
(cardinality × modify-result × post-state, G1–G4) still runs through
Hypothesis via ``TestRetagProperties``.
"""

from __future__ import annotations

import logging
import sqlite3
import subprocess as sp
import unittest
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import (
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
    ModifyRetagRun,
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
    calls: list[tuple[str, str]],
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
    calls: list[tuple[str, str]],
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
) -> None:
    """G3 — the query targets the album we are moving AWAY from, and the
    assignment carries the identity we are moving TO. Never confused, never
    swapped."""
    for query, assignment in calls:
        if new_identity.release_id in query:
            raise AssertionError(
                f"modify query names the survivor {new_identity.release_id}: "
                f"{query!r}"
            )
        if query != retag_album_query(old_identity):
            raise AssertionError(
                "modify query is not the anchored query for the old "
                f"identity {old_identity.release_id}: {query!r}"
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
    beets.set_album_ids_for_release(MERGED, [7, 8])


def _modify(
    result: str, apply_post_state: Callable[[], None], calls: list[tuple[str, str]],
) -> Callable[[str, str], ModifyRetagRun]:
    def run(query: str, assignment: str) -> ModifyRetagRun:
        calls.append((query, assignment))
        apply_post_state()
        if result == "raises_timeout":
            raise sp.TimeoutExpired(cmd=["beets", "modify"], timeout=120)
        if result == "raises_oserror":
            raise OSError("No such file or directory: beets python")
        return ModifyRetagRun(
            returncode=0 if result == "exit_0" else 1, stdout="", stderr="",
        )

    return run


def _snapshot(
    beets: FakeBeetsDB,
) -> tuple[CurrentBeetsResolution, CurrentBeetsResolution]:
    resolutions = beets.resolve_current_releases([OLD, NEW])
    return resolutions[OLD], resolutions[NEW]


class TestRetagProperties(unittest.TestCase):
    """G1–G4 over every world, driving the real retag against the real fake."""

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
    def test_every_world_upholds_the_retag_invariants(
        self,
        old_ids: tuple[int, ...],
        new_ids: tuple[int, ...],
        modify_result: str,
        post_state: str,
    ) -> None:
        beets = library(old_album_ids=old_ids, new_album_ids=new_ids)
        old_before, new_before = _snapshot(beets)
        calls: list[tuple[str, str]] = []

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
            calls, old_identity=OLD, new_identity=NEW,
        )
        check_both_held_is_never_ready(
            result.outcome, old_before=old_before, new_before=new_before,
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
        calls: list[tuple[str, str]] = []
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

    def _unique(self, identity: ReleaseIdentity) -> CurrentBeetsUnique:
        return CurrentBeetsUnique(
            identity=identity,
            album_id=7,
            album_path="/library/album-7",
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
        with self.assertRaises(AssertionError):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD), retag_assignment(NEW))],
                old_before=self._missing(OLD),
                new_before=self._missing(NEW),
            )

    def test_modify_while_the_survivor_is_already_held_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD), retag_assignment(NEW))],
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
            )

    def test_modify_invoked_twice_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_modify_only_for_a_uniquely_held_old_id(
                [(retag_album_query(OLD), retag_assignment(NEW))] * 2,
                old_before=self._unique(OLD),
                new_before=self._missing(NEW),
            )

    def test_a_query_naming_the_survivor_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(NEW), retag_assignment(NEW))],
                old_identity=OLD, new_identity=NEW,
            )

    def test_an_unanchored_query_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_query_and_assignment_name_the_right_identity(
                [(f"mb_albumid:{MERGED}", retag_assignment(NEW))],
                old_identity=OLD, new_identity=NEW,
            )

    def test_an_assignment_naming_the_merged_away_id_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_query_and_assignment_name_the_right_identity(
                [(retag_album_query(OLD), retag_assignment(OLD))],
                old_identity=OLD, new_identity=NEW,
            )

    def test_a_ready_outcome_on_the_double_sided_merge_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_both_held_is_never_ready(
                RETAG_RETAGGED,
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
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
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(self._real_observation(
                entries=("01 Installed 1.mp3", "02 Installed 2.mp3"),
            ))
        self.assertIn("sidecar", str(caught.exception))

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
            [(retag_album_query(OLD), retag_assignment(NEW))],
            old_before=self._unique(OLD),
            new_before=self._missing(NEW),
        )
        check_query_and_assignment_name_the_right_identity(
            [(retag_album_query(OLD), retag_assignment(NEW))],
            old_identity=OLD, new_identity=NEW,
        )
        check_both_held_is_never_ready(
            RETAG_AMBIGUOUS,
            old_before=self._unique(OLD),
            new_before=self._unique(NEW),
        )


if __name__ == "__main__":
    unittest.main()
