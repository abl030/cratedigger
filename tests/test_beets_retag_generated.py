"""Generated properties for the one-album mbsync retag (#1059).

The pins in ``tests/test_beets_retag.py`` prove the exact branches; these
properties patrol the world space around them, driving the REAL
``retag_merged_album`` over every combination of (old-side resolution ×
new-side resolution × what ``mbsync`` does × the library it leaves behind).

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
G2  ``mbsync`` is invoked at most once, and only in the single world that
    authorizes a library mutation: the old id uniquely held and the new id
    not held at all. An ambiguous or absent old side, or an already-present
    new side, must never reach it.
G3  The query handed to ``mbsync`` always names the OLD identity, never the
    new one. Retagging the survivor is a no-op at best and a wrong-album
    mutation at worst.
G4  Both sides held never returns a ready outcome. Two installed albums that
    MusicBrainz now calls one release is the operator's decision.
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
    MbsyncRun,
    mbsync_album_query,
    retag_merged_album,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB
from tests.test_beets_retag import MERGED, NEW, OLD, SURVIVOR, library


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


def check_mbsync_only_for_a_uniquely_held_old_id(
    queries: list[str],
    *,
    old_before: CurrentBeetsResolution,
    new_before: CurrentBeetsResolution,
) -> None:
    """G2 — the library mutation runs at most once, and only when it may."""
    if len(queries) > 1:
        raise AssertionError(
            f"mbsync was invoked {len(queries)} times for one album: {queries!r}"
        )
    if not queries:
        return
    old_is_uniquely_held = isinstance(old_before, CurrentBeetsUnique)
    new_is_absent = isinstance(new_before, CurrentBeetsMissing)
    if not old_is_uniquely_held:
        raise AssertionError(
            "mbsync was invoked while the old id resolved "
            f"{type(old_before).__name__} — only a uniquely held old album "
            "may be retagged"
        )
    if not new_is_absent:
        raise AssertionError(
            "mbsync was invoked while the new id already resolved "
            f"{type(new_before).__name__} — retagging onto a held survivor "
            "would collide two albums under one duplicate key"
        )


def check_query_names_the_old_identity(
    queries: list[str],
    *,
    old_identity: ReleaseIdentity,
    new_identity: ReleaseIdentity,
) -> None:
    """G3 — the query targets the album we are moving AWAY from."""
    for query in queries:
        if new_identity.release_id in query:
            raise AssertionError(
                f"mbsync query names the survivor {new_identity.release_id}: "
                f"{query!r}"
            )
        if query != mbsync_album_query(old_identity):
            raise AssertionError(
                "mbsync query is not the anchored query for the old identity "
                f"{old_identity.release_id}: {query!r}"
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

#: What one ``mbsync`` invocation does. ``returncode`` is present precisely
#: because it must not decide anything: the command logs and skips a release
#: it cannot fetch and still exits 0.
MBSYNC_RESULTS = st.sampled_from(["exit_0", "exit_1", "raises_timeout", "raises_oserror"])

#: What the library looks like AFTER mbsync ran — including the worlds where
#: it did nothing, moved cleanly, moved halfway, or invented a second album.
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


def _mbsync(
    result: str, apply_post_state: Callable[[], None], queries: list[str],
) -> Callable[[str], MbsyncRun]:
    def run(query: str) -> MbsyncRun:
        queries.append(query)
        apply_post_state()
        if result == "raises_timeout":
            raise sp.TimeoutExpired(cmd=["beets", "mbsync"], timeout=120)
        if result == "raises_oserror":
            raise OSError("No such file or directory: beets python")
        return MbsyncRun(
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
        mbsync_result=MBSYNC_RESULTS,
        post_state=POST_STATES,
    )
    # The decisive worlds: a clean retag, and the one that motivated the
    # whole "exit status is not evidence" contract — mbsync exits 0 while
    # the library never moved.
    @example(
        old_ids=(7,), new_ids=(), mbsync_result="exit_0", post_state="moved",
    )
    @example(
        old_ids=(7,), new_ids=(), mbsync_result="exit_0", post_state="unchanged",
    )
    @example(
        old_ids=(7,), new_ids=(8,), mbsync_result="exit_0", post_state="moved",
    )
    def test_every_world_upholds_the_retag_invariants(
        self,
        old_ids: tuple[int, ...],
        new_ids: tuple[int, ...],
        mbsync_result: str,
        post_state: str,
    ) -> None:
        beets = library(old_album_ids=old_ids, new_album_ids=new_ids)
        old_before, new_before = _snapshot(beets)
        queries: list[str] = []

        with _silence_logs():
            result = retag_merged_album(
                beets,
                old_identity=OLD,
                new_identity=NEW,
                run_mbsync=_mbsync(
                    mbsync_result, lambda: _apply_post_state(beets, post_state),
                    queries,
                ),
            )

        old_after, new_after = _snapshot(beets)
        check_ready_only_when_rekeyable(
            result.outcome, old_after=old_after, new_after=new_after,
        )
        check_mbsync_only_for_a_uniquely_held_old_id(
            queries, old_before=old_before, new_before=new_before,
        )
        check_query_names_the_old_identity(
            queries, old_identity=OLD, new_identity=NEW,
        )
        check_both_held_is_never_ready(
            result.outcome, old_before=old_before, new_before=new_before,
        )
        self.assertTrue(result.detail, "every outcome carries a diagnostic")

    @settings(deadline=None)
    @given(
        old_ids=CARDINALITIES,
        new_ids=CARDINALITIES,
        mbsync_result=MBSYNC_RESULTS,
        post_state=POST_STATES,
        fail_on_snapshot=st.sampled_from([1, 2]),
    )
    def test_an_unreadable_authority_never_authorizes_a_rekey(
        self,
        old_ids: tuple[int, ...],
        new_ids: tuple[int, ...],
        mbsync_result: str,
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
        queries: list[str] = []
        with _silence_logs():
            result = retag_merged_album(
                resolver,
                old_identity=OLD,
                new_identity=NEW,
                run_mbsync=_mbsync(
                    mbsync_result, lambda: _apply_post_state(inner, post_state),
                    queries,
                ),
            )

        if resolver.raised:
            self.assertEqual(result.outcome, RETAG_FAILED)
            self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        else:
            # The retag path was never taken, so only one snapshot happened
            # and every answer is backed by a real observation.
            self.assertNotEqual(result.outcome, RETAG_RETAGGED)


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

    def test_mbsync_on_a_missing_old_id_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mbsync_only_for_a_uniquely_held_old_id(
                [mbsync_album_query(OLD)],
                old_before=self._missing(OLD),
                new_before=self._missing(NEW),
            )

    def test_mbsync_while_the_survivor_is_already_held_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mbsync_only_for_a_uniquely_held_old_id(
                [mbsync_album_query(OLD)],
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
            )

    def test_mbsync_invoked_twice_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_mbsync_only_for_a_uniquely_held_old_id(
                [mbsync_album_query(OLD)] * 2,
                old_before=self._unique(OLD),
                new_before=self._missing(NEW),
            )

    def test_a_query_naming_the_survivor_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_query_names_the_old_identity(
                [mbsync_album_query(NEW)], old_identity=OLD, new_identity=NEW,
            )

    def test_an_unanchored_query_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_query_names_the_old_identity(
                [f"mb_albumid:{MERGED}"], old_identity=OLD, new_identity=NEW,
            )

    def test_a_ready_outcome_on_the_double_sided_merge_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            check_both_held_is_never_ready(
                RETAG_RETAGGED,
                old_before=self._unique(OLD),
                new_before=self._unique(NEW),
            )

    def test_checkers_accept_the_legitimate_retag(self) -> None:
        """Must-still-work: a real successful retag passes every checker."""
        check_ready_only_when_rekeyable(
            RETAG_RETAGGED,
            old_after=self._missing(OLD),
            new_after=self._unique(NEW),
        )
        check_mbsync_only_for_a_uniquely_held_old_id(
            [mbsync_album_query(OLD)],
            old_before=self._unique(OLD),
            new_before=self._missing(NEW),
        )
        check_query_names_the_old_identity(
            [mbsync_album_query(OLD)], old_identity=OLD, new_identity=NEW,
        )
        check_both_held_is_never_ready(
            RETAG_AMBIGUOUS,
            old_before=self._unique(OLD),
            new_before=self._unique(NEW),
        )


if __name__ == "__main__":
    unittest.main()
