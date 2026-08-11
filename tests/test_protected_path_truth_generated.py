"""Generated patrol: an unobservable path is never reported as absent.

Issue #1063. The invariant every world below must satisfy:

    A Wrong Matches DB pointer is cleared IFF deletion was positively
    confirmed, or genuine absence was positively established.

"Positively" is the load-bearing word. ``os.path.isdir`` answers
``False`` for EACCES exactly as it does for ENOENT, so the live operator
CLI cleared pointers off eight intact FLACs and reported ``deleted``.
The property drives the REAL delete service over real directories in
real worlds — present, genuinely missing, unreadable parent, unreadable
album, non-directory, unsafe root, active job, lock contention, and a
delete that fails partway — and checks the invariant on the actual DB
rows and the actual filesystem afterwards.
"""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import unittest

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.util import observe_failed_path
from lib.wrong_match_delete_service import (
    OUTCOME_DELETE_FAILED,
    OUTCOME_DELETED,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_LOCKED,
    OUTCOME_SKIPPED_PATH_UNAVAILABLE,
    OUTCOME_SKIPPED_UNSAFE_PATH,
    WrongMatchDeleteResult,
    delete_wrong_match,
)
from tests.fakes import FakePipelineDB
from tests.helpers import SeededWrongMatch, seed_visible_wrong_match

#: Every world the property drives. Ordered so a shrink lands on the
#: simplest one that still violates the invariant.
WORLDS: tuple[str, ...] = (
    "present",
    "genuinely_missing",
    "unreadable_parent",
    "unreadable_album",
    "not_a_directory",
    "unsafe_root",
    "active_job",
    "lock_contended",
    "delete_race",
    "delete_error",
)

#: Worlds in which the folder's fate is POSITIVELY known: it was deleted,
#: or it was proven not to be there. Only these may clear the pointer.
_CLEARING_WORLDS: frozenset[str] = frozenset({
    "present",
    "genuinely_missing",
    "not_a_directory",
    "delete_race",
})

#: Worlds where the OBSERVATION itself was refused (the probe could not
#: reach the name). ``unreadable_album`` is deliberately NOT one of these:
#: a 0700 parent still lets ``stat`` answer for the child, so the album is
#: positively present and it is the deletion that fails. Keeping the two
#: apart is the whole point — each world must reach its own truthful
#: outcome, not a convenient shared one.
_UNOBSERVABLE_WORLDS: frozenset[str] = frozenset({
    "unreadable_parent",
})


def pointer_cleared_iff_positively_known(
    *,
    world: str,
    result: WrongMatchDeleteResult,
    rows_remaining: int,
    folder_present: bool,
) -> None:
    """The one invariant. Raises with a diagnosis when it is violated."""
    cleared = rows_remaining == 0
    may_clear = world in _CLEARING_WORLDS
    if cleared and not may_clear:
        raise AssertionError(
            f"world={world!r} cleared the pointer without positive evidence "
            f"(outcome={result.outcome!r}, path_missing={result.path_missing}, "
            f"deleted_path={result.deleted_path!r})"
        )
    if may_clear and not cleared:
        raise AssertionError(
            f"world={world!r} refused to clear a positively-known pointer "
            f"(outcome={result.outcome!r})"
        )
    if result.deleted_path is not None and folder_present:
        raise AssertionError(
            f"world={world!r} reported deleted_path={result.deleted_path!r} "
            "while the folder is still on disk"
        )
    if world in _UNOBSERVABLE_WORLDS:
        if result.outcome != OUTCOME_SKIPPED_PATH_UNAVAILABLE:
            raise AssertionError(
                f"world={world!r} reported {result.outcome!r} for an "
                "observation that was refused"
            )
        if result.path_missing or result.deleted_path is not None:
            raise AssertionError(
                f"world={world!r} claimed absence or deletion it never proved"
            )
        if result.success:
            raise AssertionError(
                f"world={world!r} reported success for a refused observation"
            )


def observation_never_launders_a_refusal(path: str, world: str) -> None:
    """The primitive itself must not answer 'gone' for 'I could not look'."""
    observation = observe_failed_path(path)
    if world in _UNOBSERVABLE_WORLDS and not observation.indeterminate:
        raise AssertionError(
            f"world={world!r} produced presence={observation.presence!r}; "
            "a refused probe must never be reported as absence"
        )
    if world == "genuinely_missing" and not observation.absent:
        raise AssertionError(
            "a genuinely missing path must be positively absent"
        )


def _build_world(
    db: FakePipelineDB, root: str, world: str,
) -> SeededWrongMatch:
    quarantine = "elsewhere" if world == "unsafe_root" else "wrong_matches"
    source = seed_visible_wrong_match(db, root, quarantine=quarantine)
    if world == "genuinely_missing":
        shutil.rmtree(source.path)
    elif world == "not_a_directory":
        shutil.rmtree(source.path)
        with open(source.path, "wb") as handle:
            handle.write(b"not a directory")
    elif world == "unreadable_parent":
        os.chmod(source.parent, 0o000)
    elif world == "unreadable_album":
        os.chmod(source.path, 0o000)
    elif world == "delete_error":
        # Readable and observable, but its children cannot be unlinked:
        # a genuine mid-delete failure, distinct from an unobservable path.
        os.chmod(source.path, 0o500)
    elif world == "active_job":
        db.enqueue_import_job(
            "force_import",
            request_id=source.request_id,
            payload={
                "download_log_id": source.download_log_id,
                "failed_path": source.path,
            },
        )
    elif world == "lock_contended":
        db.set_advisory_lock_result(False)
    return source


def _restore(source: SeededWrongMatch) -> None:
    """Undo every permission world so the tmp tree can be removed."""
    for path in (source.parent, source.path):
        try:
            os.chmod(path, 0o700)
        except OSError:
            continue


class TestProtectedPathTruthGenerated(unittest.TestCase):
    @example(world="unreadable_parent")
    @example(world="unreadable_album")
    @example(world="delete_error")
    @example(world="genuinely_missing")
    @example(world="present")
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(world=st.sampled_from(WORLDS))
    def test_pointer_survives_every_world_it_was_not_proven_in(
        self, world: str,
    ) -> None:
        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as root:
            source = _build_world(db, root, world)
            if world == "delete_race":
                # The folder disappears between observation and rmtree —
                # absence still ends up positively established, by the
                # delete itself.
                observed = observe_failed_path(source.path)
                self.assertTrue(observed.present)
                shutil.rmtree(source.path)
            try:
                observation_never_launders_a_refusal(source.path, world)
                result = delete_wrong_match(
                    db, source.download_log_id, require_visible=True,
                )
            finally:
                _restore(source)
            folder_present = os.path.isdir(source.path)
            pointer_cleared_iff_positively_known(
                world=world,
                result=result,
                rows_remaining=len(db.get_wrong_matches()),
                folder_present=folder_present,
            )

    def test_known_bad_checkers_trip_on_violations(self) -> None:
        """Every checker above must be able to fail (issue #1063)."""
        laundered = WrongMatchDeleteResult(
            download_log_id=1,
            outcome=OUTCOME_DELETED,
            success=True,
            path_missing=True,
            cleared_rows=1,
        )
        # The exact live defect: an unreadable world reported as deleted
        # with the pointer cleared.
        with self.assertRaises(AssertionError):
            pointer_cleared_iff_positively_known(
                world="unreadable_parent",
                result=laundered,
                rows_remaining=0,
                folder_present=True,
            )
        # A refusal that keeps the pointer but still calls itself deleted.
        with self.assertRaises(AssertionError):
            pointer_cleared_iff_positively_known(
                world="unreadable_parent",
                result=laundered,
                rows_remaining=1,
                folder_present=True,
            )
        # A deletion that claims a path it did not remove.
        with self.assertRaises(AssertionError):
            pointer_cleared_iff_positively_known(
                world="present",
                result=WrongMatchDeleteResult(
                    download_log_id=1,
                    outcome=OUTCOME_DELETED,
                    success=True,
                    deleted_path="/kept",
                    cleared_rows=1,
                ),
                rows_remaining=0,
                folder_present=True,
            )
        # Fail-closed in the other direction: refusing to clear a
        # positively-known absence would strand the operator's queue.
        with self.assertRaises(AssertionError):
            pointer_cleared_iff_positively_known(
                world="genuinely_missing",
                result=WrongMatchDeleteResult(
                    download_log_id=1,
                    outcome=OUTCOME_SKIPPED_PATH_UNAVAILABLE,
                ),
                rows_remaining=1,
                folder_present=False,
            )
        with self.assertRaises(AssertionError):
            observation_never_launders_a_refusal("/definitely/not/there", "unreadable_parent")
        with self.assertRaises(AssertionError):
            observation_never_launders_a_refusal(
                tempfile.gettempdir(), "genuinely_missing")

    def test_every_world_reaches_its_own_outcome(self) -> None:
        """Entropy guard: the worlds are distinct, not one world ten times."""
        expected: dict[str, str] = {
            "present": OUTCOME_DELETED,
            "genuinely_missing": OUTCOME_DELETED,
            "not_a_directory": OUTCOME_DELETED,
            "delete_race": OUTCOME_DELETED,
            "unreadable_parent": OUTCOME_SKIPPED_PATH_UNAVAILABLE,
            "unreadable_album": OUTCOME_DELETE_FAILED,
            "unsafe_root": OUTCOME_SKIPPED_UNSAFE_PATH,
            "active_job": OUTCOME_SKIPPED_ACTIVE_JOB,
            "lock_contended": OUTCOME_SKIPPED_LOCKED,
            "delete_error": OUTCOME_DELETE_FAILED,
        }
        for world, outcome in expected.items():
            with self.subTest(world=world):
                db = FakePipelineDB()
                with tempfile.TemporaryDirectory() as root:
                    source = _build_world(db, root, world)
                    if world == "delete_race":
                        shutil.rmtree(source.path)
                    try:
                        result = delete_wrong_match(
                            db, source.download_log_id, require_visible=True,
                        )
                    finally:
                        _restore(source)
                self.assertEqual(result.outcome, outcome)

    def test_fixture_permissions_actually_deny_this_uid(self) -> None:
        """Rule C: the unreadable world must really be unreadable here.

        Root bypasses mode bits. If the suite ever runs as root the
        permission worlds silently become ordinary ones, so prove the
        denial with the real syscall before trusting any of them.
        """
        with tempfile.TemporaryDirectory() as root:
            parent = os.path.join(root, "wrong_matches")
            album = os.path.join(parent, "Album")
            os.makedirs(album)
            os.chmod(parent, 0o000)
            try:
                self.assertEqual(
                    stat.S_IMODE(os.stat(parent).st_mode), 0o000)
                with self.assertRaises(PermissionError):
                    os.stat(album)
                self.assertTrue(observe_failed_path(album).indeterminate)
            finally:
                os.chmod(parent, 0o700)
