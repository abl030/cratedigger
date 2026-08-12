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

import itertools
import os
import shutil
import socket
import stat
import tempfile
import unittest
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import ClassVar, NamedTuple

import msgspec
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.fs_authority import DirectoryObservation, unreadable_reason_text
from lib.util import observe_failed_path
from lib.wrong_match_cleanup_service import (
    _observe_first_existing as _cleanup_observe_first_existing,
)
from lib.wrong_match_delete_service import (
    OUTCOME_DELETE_FAILED,
    OUTCOME_DELETED,
    OUTCOME_PATH_MISSING,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_LOCKED,
    OUTCOME_SKIPPED_PATH_UNAVAILABLE,
    OUTCOME_SKIPPED_UNSAFE_PATH,
    WrongMatchDeleteResult,
    delete_wrong_match,
)
from lib.wrong_match_delete_service import (
    _observe_first_existing as _delete_observe_first_existing,
)
from lib.wrong_matches import _observed_candidates
from tests.fakes import FakePipelineDB
from tests.helpers import (
    SeededWrongMatch,
    make_request_row,
    seed_visible_wrong_match,
)
from tests.node_jsonl_worker import NodeJsonlWorker
from web.wrong_match_file_service import (
    WrongMatchSourceUnavailable,
    build_wrong_match_explorer,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

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
            "genuinely_missing": OUTCOME_PATH_MISSING,
            "not_a_directory": OUTCOME_PATH_MISSING,
            "delete_race": OUTCOME_PATH_MISSING,
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


# ---------------------------------------------------------------------------
# The aggregation rule: a refused probe outranks absence.
#
# Four helpers reduce SEVERAL candidate names to one observation, and every
# one of them states this rule in its docstring: the first PRESENT name
# wins; otherwise a refused probe must beat an absent one. Nothing
# constrained it, and two mutants proved it — dropping the "remember the
# refusal" branch, and reverting to "return the last/direct observation" —
# survived the whole suite (issue #1063 review T1.1).
#
# The world it reopens is #1063 verbatim: a legacy relative row
# ``failed_imports/Album`` whose direct probe answers ENOENT while the
# slskd-root probe answers EACCES. Laundered to absent, that clears the
# pointer off an intact folder.
# ---------------------------------------------------------------------------

CANDIDATE_WORLDS: tuple[str, ...] = ("present", "absent", "unreadable")


def expected_aggregate(worlds: Sequence[str]) -> str:
    """The rule itself, written once, in the order it must be applied."""
    if "present" in worlds:
        return "present"
    if "unreadable" in worlds:
        return "indeterminate"
    return "absent"


def assert_aggregate_obeys_the_refusal_rule(
    *,
    aggregator: str,
    worlds: Sequence[str],
    observation: DirectoryObservation,
    expected_path: str | None,
) -> None:
    """Checker: one aggregator, one candidate sequence, one verdict."""
    expected = expected_aggregate(worlds)
    if observation.presence != expected:
        raise AssertionError(
            f"{aggregator}: candidates {list(worlds)} aggregated to "
            f"{observation.presence!r}, expected {expected!r}"
            + (
                " — a refused probe was laundered into absence"
                if expected == "indeterminate" else ""
            )
        )
    if expected == "present" and observation.path != expected_path:
        raise AssertionError(
            f"{aggregator}: resolved {observation.path!r}, expected the "
            f"FIRST present candidate {expected_path!r}"
        )
    if expected != "present" and observation.path is not None:
        raise AssertionError(
            f"{aggregator}: reported a path {observation.path!r} for a "
            f"{expected!r} aggregate"
        )


def _candidate_dir(root: str, index: int, world: str) -> str:
    """Build one real candidate directory for the given world."""
    parent = os.path.join(root, f"root{index}")
    os.makedirs(parent, exist_ok=True)
    path = os.path.join(parent, "wrong_matches", "Album")
    if world in ("present", "unreadable"):
        os.makedirs(path, exist_ok=True)
    if world == "unreadable":
        os.chmod(os.path.dirname(path), 0o000)
    return path


def _restore_candidates(paths: Sequence[str]) -> None:
    for path in paths:
        try:
            os.chmod(os.path.dirname(path), 0o700)
        except OSError:
            continue


class TestCandidateAggregationGenerated(unittest.TestCase):
    """Every aggregator of several candidate names obeys the same rule."""

    #: name -> callable taking the ordered candidate paths.
    AGGREGATORS: ClassVar[dict[str, Callable[[list[str]], DirectoryObservation]]] = {
        "lib.wrong_matches._observed_candidates":
            lambda paths: _observed_candidates(list(paths))[0],
        "lib.wrong_match_delete_service._observe_first_existing":
            _delete_observe_first_existing,
        "lib.wrong_match_cleanup_service._observe_first_existing":
            _cleanup_observe_first_existing,
    }

    @example(worlds=["absent", "unreadable"])
    @example(worlds=["unreadable", "absent"])
    @example(worlds=["absent", "absent"])
    @example(worlds=["unreadable", "present"])
    @example(worlds=["present", "unreadable"])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(
        st.sampled_from(CANDIDATE_WORLDS), min_size=1, max_size=4,
    ))
    def test_every_aggregator_agrees_on_the_refusal_rule(
        self, worlds: list[str],
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            paths = [
                _candidate_dir(root, index, world)
                for index, world in enumerate(worlds)
            ]
            expected_path = next(
                (
                    os.path.abspath(path)
                    for path, world in zip(paths, worlds, strict=True)
                    if world == "present"
                ),
                None,
            )
            try:
                for name, aggregate in self.AGGREGATORS.items():
                    assert_aggregate_obeys_the_refusal_rule(
                        aggregator=name,
                        worlds=worlds,
                        observation=aggregate(paths),
                        expected_path=expected_path,
                    )
            finally:
                _restore_candidates(paths)

    @example(worlds=["absent", "unreadable"])
    @example(worlds=["unreadable", "absent"])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(
        st.sampled_from(CANDIDATE_WORLDS), min_size=1, max_size=3,
    ))
    def test_search_dir_fallback_obeys_the_same_rule(
        self, worlds: list[str],
    ) -> None:
        """``lib.util.observe_failed_path``'s own aggregation.

        A relative legacy name is probed directly (absent — the name does
        not exist relative to the process cwd) and then under every
        configured search directory. This is the exact live shape the
        mutants reopened.
        """
        with tempfile.TemporaryDirectory() as root:
            bases: list[str] = []
            for index, world in enumerate(worlds):
                base = os.path.join(root, f"base{index}")
                quarantine = os.path.join(base, "wrong_matches")
                if world in ("present", "unreadable"):
                    os.makedirs(os.path.join(quarantine, "Album"))
                else:
                    os.makedirs(base)
                if world == "unreadable":
                    os.chmod(quarantine, 0o000)
                bases.append(base)
            expected_path = next(
                (
                    os.path.join(base, "wrong_matches", "Album")
                    for base, world in zip(bases, worlds, strict=True)
                    if world == "present"
                ),
                None,
            )
            try:
                assert_aggregate_obeys_the_refusal_rule(
                    aggregator="lib.util.observe_failed_path",
                    # The direct probe is one more absent candidate.
                    worlds=["absent", *worlds],
                    observation=observe_failed_path(
                        os.path.join("wrong_matches", "Album"),
                        search_dirs=bases,
                    ),
                    expected_path=expected_path,
                )
            finally:
                for base in bases:
                    try:
                        os.chmod(os.path.join(base, "wrong_matches"), 0o700)
                    except OSError:
                        continue

    def test_known_bad_aggregation_checkers_trip(self) -> None:
        """The checker must fail on both mutants it exists to catch."""
        absent = DirectoryObservation(presence="absent", code="missing")
        refused = DirectoryObservation(
            presence="indeterminate", code="open_failed", errno_symbol="EACCES")
        present = DirectoryObservation(presence="present", path="/a")

        # Mutant A/B shape: a refusal anywhere in the sequence laundered
        # into absence, whichever end it sat at.
        for worlds in (["absent", "unreadable"], ["unreadable", "absent"]):
            with self.assertRaises(AssertionError):
                assert_aggregate_obeys_the_refusal_rule(
                    aggregator="mutant", worlds=worlds,
                    observation=absent, expected_path=None,
                )
        # Fail-closed in the other direction: a present candidate must not
        # be downgraded to a refusal.
        with self.assertRaises(AssertionError):
            assert_aggregate_obeys_the_refusal_rule(
                aggregator="mutant", worlds=["unreadable", "present"],
                observation=refused, expected_path="/a",
            )
        # The wrong present candidate.
        with self.assertRaises(AssertionError):
            assert_aggregate_obeys_the_refusal_rule(
                aggregator="mutant", worlds=["present"],
                observation=present, expected_path="/b",
            )
        # A non-present aggregate must not carry a path.
        with self.assertRaises(AssertionError):
            assert_aggregate_obeys_the_refusal_rule(
                aggregator="mutant", worlds=["absent"],
                observation=DirectoryObservation(
                    presence="absent", path="/a", code="missing"),
                expected_path=None,
            )

    def test_the_rule_itself_is_ordered(self) -> None:
        self.assertEqual(expected_aggregate(["absent", "unreadable"]), "indeterminate")
        self.assertEqual(expected_aggregate(["unreadable", "present"]), "present")
        self.assertEqual(expected_aggregate(["absent"]), "absent")


# ---------------------------------------------------------------------------
# The explorer is a DECISION surface: the operator reads it before choosing
# to delete. An entry we were refused is not an entry that is not there, so
# a listing that hides refusals is an inducement to destroy an intact album
# (issue #1063 F1).
# ---------------------------------------------------------------------------

ENTRY_WORLDS: tuple[str, ...] = (
    "readable_audio",
    "readable_other",
    "unreadable_file",
    "unreadable_dir",
    # A dangling symlink, enumerated by the scan and then NOT read.
    #
    # Note what actually happens, because the obvious premise is wrong:
    # ``open_regular_relative`` is ``O_NOFOLLOW``, so it never reaches
    # the dangling target and never sees ENOENT. It gets **ELOOP ->
    # ``unsafe_symlink``** — the identical code it would get for a
    # symlink pointing at a perfectly readable file. That is correct
    # issue #868 containment policy (a symlink is not authority,
    # whatever it points at).
    #
    # Before issue #1086 the entry was silently excluded from the
    # refusal count — the exact category error the issue fixes:
    # ``refusal_is_indeterminate("unsafe_symlink")`` answers ``False``
    # (a containment verdict, not a world failure) and the old gate
    # asked that question instead of ``errno_proves_absence``. It is
    # now a COUNTED, honestly-worded containment refusal, same as the
    # explorer's other refused worlds below.
    "vanished",
    # A Unix-domain socket: refused via ENXIO before ``S_ISREG`` even
    # runs (issue #868's ``not_regular_file``). The other silently-
    # dropped code the #1086 predicate swap fixes, alongside ELOOP.
    "unreadable_special",
)


class _ExplorerWorld(NamedTuple):
    """One real quarantine album on disk plus the handles to drive it."""

    album: str
    unreadable: tuple[str, ...]
    cfg: CratediggerConfig
    log_id: int
    entry: Mapping[str, object]


def build_explorer_world(
    db: FakePipelineDB, root: str, worlds: Sequence[str],
) -> _ExplorerWorld:
    """Materialize one real album whose entries are readable or refused."""
    db.seed_request(make_request_row(id=1, mb_release_id="mbid-1"))
    album = os.path.join(root, "failed_imports", "Album")
    os.makedirs(album)
    unreadable: list[str] = []
    for index, world in enumerate(worlds):
        if world == "readable_audio":
            path = os.path.join(album, f"{index:02d} track.mp3")
            with open(path, "wb") as handle:
                handle.write(b"\x00" * 32)
        elif world == "readable_other":
            path = os.path.join(album, f"{index:02d} notes.txt")
            with open(path, "wb") as handle:
                handle.write(b"notes")
        elif world == "unreadable_file":
            path = os.path.join(album, f"{index:02d} locked.mp3")
            with open(path, "wb") as handle:
                handle.write(b"\x00" * 32)
            os.chmod(path, 0o000)
            unreadable.append(path)
        elif world == "vanished":
            # Enumerated, then declined as a symlink (ELOOP under
            # ``O_NOFOLLOW``) — counted as a refusal since issue #1086.
            os.symlink(
                os.path.join(album, f"{index:02d} gone.mp3"),
                os.path.join(album, f"{index:02d} dangling.mp3"),
            )
        elif world == "unreadable_special":
            # A Unix-domain socket: ``O_NOFOLLOW`` answers ENXIO before a
            # descriptor exists (issue #868's ``not_regular_file``), and
            # that refusal is counted since issue #1086 too.
            sock = socket.socket(socket.AF_UNIX)
            try:
                sock.bind(os.path.join(album, f"{index:02d} weird.mp3"))
            finally:
                sock.close()
        else:
            path = os.path.join(album, f"{index:02d} locked-dir")
            os.makedirs(path)
            os.chmod(path, 0o000)
            unreadable.append(path)
    log_id = db.log_download(
        1,
        outcome="rejected",
        validation_result={"failed_path": album},
    )
    entry = db.get_download_log_entry(log_id)
    assert entry is not None
    return _ExplorerWorld(
        album=album,
        unreadable=tuple(unreadable),
        # The REAL config type, as the sibling generated test in
        # ``tests/test_path_authority_generated.py`` does: a
        # ``SimpleNamespace`` answers to any attribute name, so a renamed
        # quarantine-root field would leave every property here green
        # while production consulted different roots.
        cfg=CratediggerConfig(
            slskd_download_dir=root,
            beets_staging_dir=os.path.join(root, "staging"),
            processing_dir=os.path.join(root, "processing"),
        ),
        log_id=log_id,
        entry=entry,
    )


def assert_explorer_listing_is_honest(
    *,
    worlds: Sequence[str],
    payload: dict[str, object],
) -> None:
    """A listing never claims completeness it did not earn."""
    readable = sum(
        1 for world in worlds
        if world in ("readable_audio", "readable_other")
    )
    refused = sum(
        1 for world in worlds
        if world in (
            "unreadable_file", "unreadable_dir",
            # Containment refusals (issue #1086): neither proves absence,
            # so both count the same as an ordinary storage refusal.
            "vanished", "unreadable_special",
        )
    )
    unreadable_count = payload.get("unreadable_entry_count")
    if unreadable_count != refused:
        raise AssertionError(
            f"worlds {list(worlds)} recorded {unreadable_count} refusals, "
            f"expected {refused}"
        )
    if refused and payload.get("partial") is not True:
        raise AssertionError(
            f"worlds {list(worlds)} hid {refused} refused entries behind "
            "a complete-looking listing"
        )
    if not refused and payload.get("partial") is not False:
        raise AssertionError(
            f"worlds {list(worlds)} reported a partial listing with nothing "
            "truncated and nothing refused"
        )
    if refused and not payload.get("unreadable_reason"):
        raise AssertionError(
            f"worlds {list(worlds)} recorded refusals without naming one"
        )
    # The load-bearing case: zero readable entries plus a refusal must
    # never render as a confident empty folder.
    empty_claim = readable == 0 and payload.get("status") == "ok"
    if refused and empty_claim:
        raise AssertionError(
            f"worlds {list(worlds)} claimed an intact folder is empty "
            f"(status={payload.get('status')!r}, "
            f"audio_file_count={payload.get('audio_file_count')!r})"
        )
    if not refused and payload.get("status") != "ok":
        raise AssertionError(
            f"worlds {list(worlds)} refused nothing yet reported "
            f"status={payload.get('status')!r}"
        )
    audio = sum(1 for world in worlds if world == "readable_audio")
    if payload.get("audio_file_count") != audio:
        raise AssertionError(
            f"worlds {list(worlds)} listed "
            f"{payload.get('audio_file_count')!r} audio files, expected {audio}"
        )


class TestExplorerRefusalHonestyGenerated(unittest.TestCase):
    """The real explorer over real trees of readable/unreadable entries."""

    @example(worlds=["unreadable_file"])
    @example(worlds=["unreadable_file", "unreadable_file", "unreadable_dir"])
    @example(worlds=["readable_audio", "unreadable_file"])
    @example(worlds=["readable_audio"])
    @example(worlds=["vanished"])
    @example(worlds=["readable_audio", "vanished"])
    @example(worlds=["vanished", "unreadable_file"])
    @example(worlds=[])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(
        st.sampled_from(ENTRY_WORLDS), min_size=0, max_size=4,
    ))
    def test_real_explorer_never_hides_a_refusal(
        self, worlds: list[str],
    ) -> None:
        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as root:
            world = build_explorer_world(db, root, worlds)
            try:
                payload = build_wrong_match_explorer(
                    download_log_id=world.log_id,
                    entry=world.entry,
                    cfg=world.cfg,
                )
            finally:
                for path in world.unreadable:
                    os.chmod(path, 0o700)
            assert_explorer_listing_is_honest(worlds=worlds, payload=payload)

    def test_known_bad_explorer_checker_trips(self) -> None:
        """The exact pre-fix payload must fail this checker."""
        pre_fix = {
            "status": "ok",
            "audio_file_count": 0,
            "other_file_count": 0,
            "partial": False,
            "truncated_reason": None,
            "unreadable_entry_count": 0,
            "unreadable_reason": None,
            "files": [],
        }
        with self.assertRaises(AssertionError):
            assert_explorer_listing_is_honest(
                worlds=["unreadable_file", "unreadable_file", "unreadable_dir"],
                payload=pre_fix,
            )
        # Counted but still presented as a confident empty folder.
        with self.assertRaises(AssertionError):
            assert_explorer_listing_is_honest(
                worlds=["unreadable_file"],
                payload={**pre_fix, "unreadable_entry_count": 1,
                         "partial": True, "unreadable_reason": "x: denied"},
            )
        # Counted, flagged, but no reason named.
        with self.assertRaises(AssertionError):
            assert_explorer_listing_is_honest(
                worlds=["unreadable_file"],
                payload={**pre_fix, "unreadable_entry_count": 1,
                         "partial": True, "status": "unavailable"},
            )
        # The ``vanished`` world's own mutant, post-#1086: an entry the
        # scan enumerated and then declined on CONTAINMENT grounds (ELOOP
        # -> ``unsafe_symlink``, per issue #868 — not ENOENT;
        # ``O_NOFOLLOW`` never reaches the target) must now be COUNTED.
        # The old ``pre_fix`` shape (0 refusals, a confident "ok" empty
        # listing) is exactly the pre-#1086 defect for this world: it is
        # what ``if refusal_is_indeterminate(exc.code) is True:`` produces
        # for ``unsafe_symlink`` (``False``), silently dropping the entry.
        with self.assertRaises(AssertionError):
            assert_explorer_listing_is_honest(
                worlds=["vanished"], payload=pre_fix,
            )
        # Must still work: the honest answer is a counted, honestly-worded
        # containment refusal — no other world in ``ENTRY_WORLDS`` reaches
        # ``unsafe_symlink``.
        assert_explorer_listing_is_honest(
            worlds=["vanished"],
            payload={
                **pre_fix, "unreadable_entry_count": 1,
                "partial": True, "status": "unavailable",
                "unreadable_reason": (
                    f"02 dangling.mp3: {unreadable_reason_text('unsafe_symlink')}"
                ),
            },
        )
        # Fail-closed the other way: an honest complete listing must pass.
        assert_explorer_listing_is_honest(
            worlds=["readable_audio"],
            payload={**pre_fix, "audio_file_count": 1},
        )


# ---------------------------------------------------------------------------
# Composition: the real producer's payload through the real browser code.
#
# Every fact above was true of the SERVER and none of it reached the
# operator. ``build_wrong_match_explorer`` answered 200 with
# ``status: "unavailable"``, the route passed it through, and
# ``web/js/wrong-matches.js`` threw it away because its gate read
# ``status !== 'ok'`` — so the panel said "Failed to load file explorer"
# with a Retry button that can never succeed on an unreadable tree, and
# the authored honest copy was unreachable code. Module-scope tests on
# both halves were green throughout. This is the widest-boundary rule:
# the invariant lives in the producer/consumer PAIR, so the pin and the
# property drive the real writer and the real reader over one payload.
# ---------------------------------------------------------------------------

_EXPLORER_BROWSER_WORKER = """
import { __test__ } from './web/js/wrong-matches.js';

function freshMount(logId) {
  const mount = { innerHTML: '' };
  globalThis.document = {
    getElementById(id) {
      return id === `wm-explorer-${logId}` ? mount : null;
    },
  };
  globalThis.setTimeout = (fn) => { fn(); return 0; };
  return mount;
}

async function handle(operation, payload) {
  if (operation === 'renderable_statuses') {
    return Array.from(__test__.EXPLORER_RENDERABLE_STATUSES);
  }
  if (operation === 'explorer') {
    const logId = Number(payload.log_id);
    const mount = freshMount(logId);
    globalThis.fetch = async () => ({
      ok: Number(payload.http_status) < 400,
      status: Number(payload.http_status),
      json: async () => payload.body,
    });
    await __test__.maybeLoadWrongMatchExplorer(logId, { open: true });
    return { html: mount.innerHTML };
  }
  throw new Error(`unknown operation ${operation}`);
}
"""

#: The copy the panel owes an operator whose listing is incomplete.
_LOAD_FAILURE_COPY = "Failed to load file explorer"
#: A WORLD-FAILURE per-entry refusal, and the whole-root 503 envelope
#: (``web/wrong_match_file_service.py``'s own "Wrong-match files could
#: not be read: …") both still use this exact phrase.
_REFUSAL_COPY = "could not be read"
#: A CONTAINMENT per-entry refusal (symlink/socket/FIFO/device node)
#: gets its OWN lead sentence (issue #1086 review) — it must never say
#: "could not be read", the wording a world failure a retry might clear
#: owns. ``assert_browser_told_the_truth`` enforces this as EXCLUSIVITY,
#: not "either is acceptable": whichever kind the FIRST refused entry in
#: ``worlds`` actually is, that copy must appear and the OTHER copy must
#: not. A permissive "either" check would pass a browser that always used
#: the wrong wording for one of the two kinds — exactly the shape of the
#: #1086 review's blocker 1 (the empty-state paragraph used
#: ``_REFUSAL_COPY`` unconditionally, so a containment world satisfied
#: the old OR-check while still failing the doctrine this constant's own
#: docstring states).
_CONTAINMENT_REFUSAL_COPY = "refused (not read)"
_NOT_EMPTY_COPY = "NOT evidence that the folder is empty"

#: Entry kinds refused for a WORLD reason (EACCES) vs a CONTAINMENT
#: reason (a symlink or a special file) — the same split
#: ``build_explorer_world`` documents at each kind's definition.
_WORLD_FAILURE_ENTRY_WORLDS: frozenset[str] = frozenset(
    {"unreadable_file", "unreadable_dir"})
_CONTAINMENT_ENTRY_WORLDS: frozenset[str] = frozenset(
    {"vanished", "unreadable_special"})


def _first_refusal_kind(worlds: Sequence[str]) -> str | None:
    """Which kind the FIRST refused entry in ``worlds`` actually is.

    Both the explorer and ``lib.beets_distance`` report only the FIRST
    refusal they hit — ``unreadable_reason``/``unreadable_is_containment``
    name one entry, not every one. ``build_explorer_world`` names each
    entry ``f"{index:02d} ..."`` in ``worlds`` order, and the real
    explorer's ``scan_directory`` sorts names before its per-entry loop
    (``names.sort()``), so the zero-padded index makes the real per-entry
    SCAN order equal to ``worlds`` list order — this function can
    therefore predict, from the world list alone, which kind the browser
    will actually be told about. Returns ``None`` when nothing in
    ``worlds`` is a refusal.
    """
    for world in worlds:
        if world in _WORLD_FAILURE_ENTRY_WORLDS:
            return "world_failure"
        if world in _CONTAINMENT_ENTRY_WORLDS:
            return "containment"
    return None


def assert_browser_told_the_truth(
    *,
    worlds: Sequence[str],
    payload: Mapping[str, object],
    html: str,
) -> None:
    """What the OPERATOR sees, composed from the real payload and real JS."""
    readable = sum(
        1 for world in worlds
        if world in ("readable_audio", "readable_other")
    )
    refused = sum(
        1 for world in worlds
        if world in (
            "unreadable_file", "unreadable_dir",
            "vanished", "unreadable_special",
        )
    )
    if _LOAD_FAILURE_COPY in html:
        raise AssertionError(
            f"worlds {list(worlds)} produced a renderable "
            f"status={payload.get('status')!r} payload the browser rejected "
            "as a load failure"
        )
    kind = _first_refusal_kind(worlds)
    if kind == "world_failure":
        if _REFUSAL_COPY not in html:
            raise AssertionError(
                f"worlds {list(worlds)}: the first refusal is a WORLD "
                f"FAILURE and the browser never said {_REFUSAL_COPY!r}"
            )
        if _CONTAINMENT_REFUSAL_COPY in html:
            raise AssertionError(
                f"worlds {list(worlds)}: the first refusal is a WORLD "
                f"FAILURE but the browser used the containment wording "
                f"{_CONTAINMENT_REFUSAL_COPY!r} — that wording must be "
                "reserved for a symlink/socket/FIFO/device-node refusal"
            )
    elif kind == "containment":
        if _CONTAINMENT_REFUSAL_COPY not in html:
            raise AssertionError(
                f"worlds {list(worlds)}: the first refusal is a "
                f"CONTAINMENT decision and the browser never said "
                f"{_CONTAINMENT_REFUSAL_COPY!r}"
            )
        if _REFUSAL_COPY in html:
            raise AssertionError(
                f"worlds {list(worlds)}: the first refusal is a "
                f"CONTAINMENT decision but the browser used the "
                f"world-failure wording {_REFUSAL_COPY!r} — a retry can "
                "never satisfy a containment refusal"
            )
    if refused and readable == 0 and _NOT_EMPTY_COPY not in html:
        raise AssertionError(
            f"worlds {list(worlds)} rendered an intact-but-unreadable folder "
            "without denying it is empty"
        )
    if not refused and (
        _REFUSAL_COPY in html
        or _CONTAINMENT_REFUSAL_COPY in html
        or _NOT_EMPTY_COPY in html
    ):
        raise AssertionError(
            f"worlds {list(worlds)} refused nothing yet the browser claimed "
            "an incomplete listing"
        )


class TestExplorerReachesTheOperatorGenerated(unittest.TestCase):
    """Real producer payload -> real ``web/js/wrong-matches.js`` render."""

    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_EXPLORER_BROWSER_WORKER, cwd=REPO_ROOT)
        self.addCleanup(self.worker.close)
        # ``_entryExplorerState`` is module-scoped in the browser module, so
        # each request needs its own id or the second render short-circuits.
        self._ids = itertools.count(9000)

    def _render(self, body: Mapping[str, object], status: int) -> str:
        result = self.worker.request("explorer", {
            "log_id": next(self._ids),
            "http_status": status,
            "body": body,
        })
        if not isinstance(result, dict):
            raise TypeError(f"explorer worker returned {type(result).__name__}")
        html = result.get("html")
        if not isinstance(html, str):
            raise TypeError("explorer worker returned no html")
        return html

    @example(worlds=["unreadable_file"])
    @example(worlds=["unreadable_file", "unreadable_dir"])
    @example(worlds=["readable_audio", "unreadable_file"])
    @example(worlds=["readable_other", "unreadable_file"])
    @example(worlds=["readable_audio"])
    @example(worlds=["vanished"])
    @example(worlds=["readable_audio", "vanished"])
    @example(worlds=[])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(
        st.sampled_from(ENTRY_WORLDS), min_size=0, max_size=4,
    ))
    def test_every_producible_payload_renders_honestly(
        self, worlds: list[str],
    ) -> None:
        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as root:
            world = build_explorer_world(db, root, worlds)
            try:
                payload = build_wrong_match_explorer(
                    download_log_id=world.log_id,
                    entry=world.entry,
                    cfg=world.cfg,
                )
            finally:
                for path in world.unreadable:
                    os.chmod(path, 0o700)
        # The vocabulary contract: every status this producer can emit is
        # one the consumer agrees to render. Both sides are asked, neither
        # is hard-coded here.
        renderable = self.worker.request("renderable_statuses", None)
        assert isinstance(renderable, list)
        self.assertIn(
            payload["status"], renderable,
            "the producer emitted a status the browser will not render",
        )
        html = self._render(payload, 200)
        assert_browser_told_the_truth(
            worlds=worlds, payload=payload, html=html)

    def test_a_refused_root_reaches_the_operator_with_its_reason(self) -> None:
        """Rule C pin: the 503 body comes from the producer that raises it.

        A refusal of the WHOLE root is the one explorer world that is not
        a 200 payload. The route answers ``h._error(str(exc), 503)``, and
        the browser used to discard the message entirely — leaving an
        operator with a bare "Failed to load" over a world that needs
        fixing.
        """
        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as root:
            world = build_explorer_world(db, root, ["readable_audio"])
            parent = os.path.dirname(world.album)
            os.chmod(parent, 0o000)
            try:
                with self.assertRaises(WrongMatchSourceUnavailable) as caught:
                    build_wrong_match_explorer(
                        download_log_id=world.log_id,
                        entry=world.entry,
                        cfg=world.cfg,
                    )
            finally:
                os.chmod(parent, 0o700)
        # Exactly the envelope ``web/routes/imports.py`` writes for it.
        reason = str(caught.exception)
        html = self._render({"error": reason}, 503)
        self.assertIn(_LOAD_FAILURE_COPY, html)
        self.assertIn(_REFUSAL_COPY, html)
        self.assertIn("Retry", html)

    def test_known_bad_browser_checker_trips(self) -> None:
        """The checker must fail on the exact pre-fix render."""
        unavailable = {"status": "unavailable", "unreadable_entry_count": 1}
        # 1. The shipped defect: a renderable payload rejected as a load
        #    failure.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["unreadable_file"],
                payload=unavailable,
                html="<div>Failed to load file explorer. <button>Retry</button></div>",
            )
        # 2. Rendered, but silent about the refusal.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["unreadable_file"],
                payload=unavailable,
                html="<div>No audio files found in this folder.</div>",
            )
        # 3. Named the refusal but still let "empty" stand.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["unreadable_file"],
                payload=unavailable,
                html="<div>1 entry could not be read</div>",
            )
        # 4. Must still work in the other direction: an ordinary complete
        #    listing may not claim an incomplete one.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["readable_audio"],
                payload={"status": "ok", "unreadable_entry_count": 0},
                html="<div>1 entry could not be read</div>",
            )
        # 4b. The ``vanished`` world's own mutant, at the browser, post-
        #     #1086: an entry declined on containment grounds must be
        #     rendered as a refusal, not silently reported "ok" and empty
        #     — the shape ``refusal_is_indeterminate`` produced pre-fix.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["vanished"],
                payload={"status": "ok", "unreadable_entry_count": 0},
                html="<div>No audio files found in this folder.</div>",
            )
        # 4c. The exact blocker-1 shape from the #1086 review: the browser
        #     correctly disclosed the refusal, but with the WORLD-FAILURE
        #     wording for what is actually a CONTAINMENT decision — the
        #     old permissive "either copy" check accepted this.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["vanished"],
                payload={"status": "unavailable", "unreadable_entry_count": 1},
                html=(
                    "<div>1 entry could not be read. This is NOT evidence "
                    "that the folder is empty.</div>"
                ),
            )
        # 4d. The mirror mutant: a WORLD-FAILURE refusal rendered with the
        #     containment wording, implying a security decision over an
        #     ordinary EACCES a retry could clear.
        with self.assertRaises(AssertionError):
            assert_browser_told_the_truth(
                worlds=["unreadable_file"],
                payload={"status": "unavailable", "unreadable_entry_count": 1},
                html=(
                    "<div>1 entry was refused (not read) as a containment "
                    "decision. This is NOT evidence that the folder is "
                    "empty.</div>"
                ),
            )
        assert_browser_told_the_truth(
            worlds=["vanished"],
            payload={"status": "unavailable", "unreadable_entry_count": 1},
            html=(
                "<div>1 entry was refused (not read) as a containment "
                "decision. This is NOT evidence that the folder is "
                "empty.</div>"
            ),
        )
        assert_browser_told_the_truth(
            worlds=["unreadable_file"],
            payload={"status": "unavailable", "unreadable_entry_count": 1},
            html=(
                "<div>1 entry could not be read. This is NOT evidence "
                "that the folder is empty.</div>"
            ),
        )
        assert_browser_told_the_truth(
            worlds=["readable_audio"],
            payload={"status": "ok", "unreadable_entry_count": 0},
            html="<div>1 track in surviving folder</div>",
        )


# ---------------------------------------------------------------------------
# The same rule, one layer further down: the beets-distance READ.
#
# ``os.walk``'s ``onerror`` and the per-file ``os.stat`` see neither of the
# refusals that actually happen here. A mode-0000 file is listed by the walk
# and stats perfectly well; only the tag read is refused, and mediafile
# converts that ``OSError`` into its own ``UnreadableFileError``. So a
# partial manifest shipped a bare distance, and an album whose files were
# ALL refused answered ``no_audio`` — HTTP 410 Gone, CLI exit 4, "the
# artifacts we wanted to compare are gone" — over intact audio.
#
# The Replace picker is where the operator chooses a pressing, so the
# property drives the real service AND the real badge formatter.
# ---------------------------------------------------------------------------

DISTANCE_FILE_WORLDS: tuple[str, ...] = (
    "readable",
    # EACCES at OPEN. Its ``OSError`` carries the filename.
    "refused",
    # EIO on an ALREADY-OPEN descriptor — the nested-virtiofs shape this
    # deployment really runs on, and the world whose absence hid a defect
    # through a pin, a property and six reviews. ``FileIO.read`` attaches
    # no filename, so the errno and the filename land on DIFFERENT links
    # of the wrapped chain and no single link carries both. Constructed
    # via a symlink to ``/proc/self/mem`` — which, post-#1086, the
    # ``lstat`` guard now refuses BEFORE that mid-read is ever attempted.
    # The world still belongs to the ``refused`` bucket (same outcome,
    # earlier gate), and the module-level pin
    # (``test_both_real_refusal_producers_are_found_and_attributed`` in
    # ``tests/test_beets_distance.py``) independently proves
    # ``os_refusal_in_chain`` still finds a GENUINE mid-read EIO when a
    # caller reaches it on a non-symlink descriptor.
    "refused_mid_read",
    # EACCES on a SUBDIRECTORY, which fails in ``os.walk``'s ``onerror``
    # rather than on a file. That is the third of the three sites
    # ``_refusal_text`` owns, and the sweep for this round found it was
    # the one no world could reach.
    "refused_dir",
    # A SELF-REFERENCING symlink. ``lstat`` never dereferences, so it
    # never even reaches the ELOOP a following ``stat`` would raise — it
    # sees "this name is a symlink" via ``S_ISLNK`` and refuses on the
    # spot (issue #1086's containment guard). Kept apart from
    # ``refused_symlink_dangling``/``refused_symlink_external`` only to
    # document that a loop, a dangling target, and a valid target
    # outside the root are ALL refused the identical way — none is
    # special-cased.
    "refused_symlink_loop",
    # A dangling symlink: the walk LISTS the name; before #1086,
    # ``os.stat`` FOLLOWED it to find ENOENT (a proven absence). The
    # ``lstat`` guard never follows, so it never discovers the target is
    # gone — it refuses the symlink itself, same as any other. This
    # dissolves the asymmetry the issue names: the SAME on-disk shape is
    # ``vanished`` in ``ENTRY_WORLDS`` above, and now BOTH ends refuse it
    # for the identical reason (a symlink, ``O_NOFOLLOW``/``lstat``,
    # never dereferenced) instead of separately reaching the same answer.
    "refused_symlink_dangling",
    # The containment gap named in the issue, made concrete: a symlink
    # to a REAL, READABLE file OUTSIDE the album folder entirely (the
    # shared FLAC fixture). Before the guard this was silently followed
    # and fingerprinted as if it were the album's own track — the
    # security-relevant half of "both ends agree by construction".
    "refused_symlink_external",
    "unparseable",
)

_DISTANCE_BADGE_WORKER = """
import { formatDistanceBadge, pickBestDistance } from './web/js/replace_picker.js';

async function handle(operation, payload) {
  if (operation === 'badge') {
    return { text: formatDistanceBadge(pickBestDistance([payload])) };
  }
  throw new Error(`unknown operation ${operation}`);
}
"""

_INCOMPLETE_BADGE_COPY = "incomplete manifest"


def assert_distance_read_is_honest(
    *,
    worlds: Sequence[str],
    outcome: str,
    partial_read: str | None,
    total_local_tracks: int | None,
) -> None:
    """A distance never hides a refusal, and never invents one."""
    readable = sum(1 for world in worlds if world == "readable")
    # Both refusal PRODUCERS count the same — that equality is the point.
    # ``refused`` is EACCES at open (errno + filename on one link);
    # ``refused_mid_read`` is EIO on an open descriptor (errno and
    # filename on different links). Only the second occurs on this
    # deployment's mount, and it is the one the guard used to drop.
    refused = sum(
        1 for world in worlds
        if world in (
            "refused", "refused_mid_read", "refused_dir",
            "refused_symlink_loop", "refused_symlink_dangling",
            "refused_symlink_external",
        )
    )
    if refused and readable == 0 and outcome == "no_audio":
        raise AssertionError(
            f"worlds {list(worlds)} reported no_audio — a definitive "
            "negative — for audio the storage refused to show"
        )
    if refused and readable == 0 and outcome != "folder_unavailable":
        raise AssertionError(
            f"worlds {list(worlds)} refused every file yet reported "
            f"outcome={outcome!r}"
        )
    if not refused and readable == 0 and outcome != "no_audio":
        raise AssertionError(
            f"worlds {list(worlds)} were fully OBSERVED and held no readable "
            f"audio, which no_audio states exactly; got outcome={outcome!r}"
        )
    if refused and readable and partial_read is None:
        raise AssertionError(
            f"worlds {list(worlds)} computed a distance over "
            f"{readable} of {readable + refused} files without flagging the "
            "incomplete manifest"
        )
    if not refused and partial_read is not None:
        raise AssertionError(
            f"worlds {list(worlds)} refused nothing yet claimed "
            f"partial_read={partial_read!r} — an unparseable file is a fact "
            "ABOUT the file and a proven absence is a fact about the name; "
            "neither is the storage refusing to answer"
        )
    if outcome == "ok" and total_local_tracks != readable:
        raise AssertionError(
            f"worlds {list(worlds)} scored {total_local_tracks!r} local "
            f"tracks, expected the {readable} it could actually read"
        )


_CONTAINMENT_QUALIFIER = "refused: symlink or special file"
_WORLD_FAILURE_QUALIFIER = "may be transient"


def assert_badge_shows_the_incompleteness(
    *,
    partial_read: str | None,
    partial_read_is_containment: bool | None = None,
    text: str,
) -> None:
    """The pressing-row badge is the operator's decision surface."""
    marked = _INCOMPLETE_BADGE_COPY in text
    if partial_read is not None and text and not marked:
        raise AssertionError(
            f"badge {text!r} presented a distance computed over an "
            f"incomplete manifest ({partial_read!r}) as an ordinary score"
        )
    if partial_read is None and marked:
        raise AssertionError(
            f"badge {text!r} claimed an incomplete manifest for a complete read"
        )
    # Issue #1086: the KIND of incompleteness must be VISIBLE in the
    # badge text itself, not just present in a hover-only title — a
    # containment refusal (a symlink/socket) must never read like an
    # ordinary transient storage failure, or vice versa.
    if partial_read is not None and text:
        expected_qualifier = (
            _CONTAINMENT_QUALIFIER if partial_read_is_containment
            else _WORLD_FAILURE_QUALIFIER
        )
        wrong_qualifier = (
            _WORLD_FAILURE_QUALIFIER if partial_read_is_containment
            else _CONTAINMENT_QUALIFIER
        )
        if expected_qualifier not in text:
            raise AssertionError(
                f"badge {text!r} did not visibly say {expected_qualifier!r} "
                f"for partial_read_is_containment={partial_read_is_containment!r}"
            )
        if wrong_qualifier in text:
            raise AssertionError(
                f"badge {text!r} said {wrong_qualifier!r}, the OTHER kind's "
                "wording"
            )


class TestDistanceReadRefusalGenerated(unittest.TestCase):
    """Real ``compute_beets_distance`` over real refused/parsed audio."""

    FIXTURE_FLAC: ClassVar[str] = os.path.join(
        os.path.dirname(__file__), "fixtures", "audio_hash", "sine_440.flac")

    def setUp(self) -> None:
        self.worker = NodeJsonlWorker(_DISTANCE_BADGE_WORKER, cwd=REPO_ROOT)
        self.addCleanup(self.worker.close)

    def _badge_text(self, result: object) -> str:
        from lib.beets_distance import BeetsDistanceResult
        assert isinstance(result, BeetsDistanceResult)
        payload = msgspec.to_builtins(result)
        rendered = self.worker.request("badge", payload)
        if not isinstance(rendered, dict):
            raise TypeError(f"badge worker returned {type(rendered).__name__}")
        text = rendered.get("text")
        if not isinstance(text, str):
            raise TypeError("badge worker returned no text")
        return text

    @example(worlds=["readable", "refused"])
    @example(worlds=["refused"])
    @example(worlds=["refused", "refused"])
    @example(worlds=["readable", "unparseable"])
    @example(worlds=["readable"])
    @example(worlds=["unparseable"])
    @example(worlds=["readable", "refused_symlink_dangling"])
    @example(worlds=["refused_symlink_dangling"])
    @example(worlds=["refused_symlink_dangling", "refused"])
    @example(worlds=["refused_mid_read"])
    @example(worlds=["readable", "refused_mid_read"])
    @example(worlds=["refused_mid_read", "refused_symlink_dangling"])
    @example(worlds=["refused_mid_read", "unparseable"])
    @example(worlds=["refused_dir"])
    @example(worlds=["readable", "refused_dir"])
    @example(worlds=["refused_symlink_loop"])
    @example(worlds=["readable", "refused_symlink_loop"])
    @example(worlds=["refused_symlink_external"])
    @example(worlds=["readable", "refused_symlink_external"])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(worlds=st.lists(
        st.sampled_from(DISTANCE_FILE_WORLDS), min_size=1, max_size=3,
    ))
    def test_a_refused_read_is_never_a_definitive_negative(
        self, worlds: list[str],
    ) -> None:
        from lib.beets_distance import compute_beets_distance
        from tests.test_beets_distance import _ok_mb_release, _present, _StubPDB

        with tempfile.TemporaryDirectory() as root:
            album = os.path.join(root, "wrong_matches", "Album")
            os.makedirs(album)
            locked: list[str] = []
            for index, world in enumerate(worlds):
                path = os.path.join(album, f"{index:02d} track.flac")
                if world == "unparseable":
                    with open(path, "wb") as handle:
                        handle.write(b"not a flac at all" * 8)
                    continue
                if world == "refused_symlink_dangling":
                    # Listed by the walk; ``lstat`` sees a symlink and
                    # refuses without ever discovering the target is gone
                    # (issue #1086 — no longer a proven absence).
                    os.symlink(os.path.join(album, f"{index:02d} gone.flac"),
                               path)
                    continue
                if world == "refused_symlink_external":
                    # A symlink to a REAL, readable file OUTSIDE the
                    # album: the containment gap the issue names. Refused
                    # the same way, never followed.
                    os.symlink(self.FIXTURE_FLAC, path)
                    continue
                if world == "refused_mid_read":
                    # Opens and stats fine; every read answers EIO. The
                    # deployment's real virtiofs shape, no flaky mount
                    # required. Constructed via a symlink, so post-#1086
                    # this is now refused at the ``lstat`` guard rather
                    # than at the mid-read — see the world's docstring.
                    os.symlink("/proc/self/mem", path)
                    continue
                if world == "refused_dir":
                    # The walk's ``onerror`` leg: a subdirectory we may
                    # not descend into.
                    subdir = os.path.join(album, f"{index:02d} disc")
                    os.makedirs(subdir)
                    os.chmod(subdir, 0o000)
                    locked.append(subdir)
                    continue
                if world == "refused_symlink_loop":
                    # A self-referencing symlink: ``lstat`` never
                    # dereferences, so it never reaches the ELOOP a
                    # following ``stat`` would raise.
                    os.symlink(path, path)
                    continue
                shutil.copy(self.FIXTURE_FLAC, path)
                if world == "refused":
                    os.chmod(path, 0o000)
                    locked.append(path)
            pdb = _StubPDB(
                download_log_entry={
                    "id": 1, "request_id": 7,
                    "validation_result": {"failed_path": album},
                },
                request={"id": 7, "mb_release_group_id": "rg-shared"},
            )
            try:
                result = compute_beets_distance(
                    1, "rel-x",
                    pdb=pdb,
                    mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
                    observe_failed_path=_present,
                )
            finally:
                for path in locked:
                    os.chmod(path, 0o700 if os.path.isdir(path) else 0o600)

        assert_distance_read_is_honest(
            worlds=worlds,
            outcome=result.outcome,
            partial_read=result.partial_read,
            total_local_tracks=result.total_local_tracks,
        )
        # …and through the outermost real adapter the operator meets it in.
        assert_badge_shows_the_incompleteness(
            partial_read=result.partial_read,
            partial_read_is_containment=result.partial_read_is_containment,
            text=self._badge_text(result),
        )

    def test_known_bad_distance_checkers_trip(self) -> None:
        """Both checkers must fail on the exact pre-fix behaviour."""
        # 1. Every file refused, reported as "there is no audio here".
        with self.assertRaises(AssertionError):
            assert_distance_read_is_honest(
                worlds=["refused"], outcome="no_audio",
                partial_read=None, total_local_tracks=None,
            )
        # 2. Partial manifest scored with no flag.
        with self.assertRaises(AssertionError):
            assert_distance_read_is_honest(
                worlds=["readable", "refused"], outcome="ok",
                partial_read=None, total_local_tracks=1,
            )
        # 3. The mirror image: a corrupt file claimed as a refusal.
        with self.assertRaises(AssertionError):
            assert_distance_read_is_honest(
                worlds=["readable", "unparseable"], outcome="ok",
                partial_read="x: bad tags", total_local_tracks=1,
            )
        # 3b. Issue #1086's regression shape: a symlink refusal (a
        #     dangling target, a self-loop, or a valid target outside the
        #     root) reported as a proven absence — the exact category
        #     error the ``lstat`` guard and the predicate swap exist to
        #     prevent. Before this delta a dangling symlink WAS reported
        #     this way (``os.stat`` followed it into ENOENT); this pin
        #     guards against that regressing.
        with self.assertRaises(AssertionError):
            assert_distance_read_is_honest(
                worlds=["readable", "refused_symlink_dangling"],
                outcome="ok", partial_read=None, total_local_tracks=1,
            )
        # 3c. …and the same false absence turning a folder holding only a
        #     symlink into ``no_audio`` (a definitive negative) instead
        #     of the ``folder_unavailable`` a refusal actually earns.
        with self.assertRaises(AssertionError):
            assert_distance_read_is_honest(
                worlds=["refused_symlink_external"], outcome="no_audio",
                partial_read=None, total_local_tracks=None,
            )
        # 4. A complete read must pass — including one whose only
        #    non-audio outcome was positively established.
        assert_distance_read_is_honest(
            worlds=["readable"], outcome="ok",
            partial_read=None, total_local_tracks=1,
        )
        # Must still work: every symlink flavour — a loop, a dangling
        # target, and a valid target outside the root — is honestly a
        # REFUSAL, never a proven absence (issue #1086).
        assert_distance_read_is_honest(
            worlds=["readable", "refused_symlink_dangling"], outcome="ok",
            partial_read="02 track.flac: symlink refused",
            total_local_tracks=1,
        )
        assert_distance_read_is_honest(
            worlds=["refused_symlink_dangling"], outcome="folder_unavailable",
            partial_read=None, total_local_tracks=None,
        )
        assert_distance_read_is_honest(
            worlds=["readable", "refused_symlink_external"], outcome="ok",
            partial_read="02 track.flac: symlink refused",
            total_local_tracks=1,
        )
        # 5. The badge checker, both directions.
        with self.assertRaises(AssertionError):
            assert_badge_shows_the_incompleteness(
                partial_read="x: Permission denied", text="best 0.07 (1/2)",
            )
        with self.assertRaises(AssertionError):
            assert_badge_shows_the_incompleteness(
                partial_read=None,
                text="best 0.07 (2/2) · incomplete manifest",
            )
        assert_badge_shows_the_incompleteness(
            partial_read="x: Permission denied",
            partial_read_is_containment=False,
            text="best 0.07 (1/2) · incomplete manifest (may be transient)",
        )
        # 5b. Issue #1086: the KIND must be VISIBLE, not merely present in
        #     ``partial_read``. A containment refusal rendered with the
        #     world-failure wording (or vice versa) is exactly the
        #     "correct string, invisible/mislabeled distinction" defect
        #     this series keeps finding.
        with self.assertRaises(AssertionError):
            assert_badge_shows_the_incompleteness(
                partial_read="x: symlink refused",
                partial_read_is_containment=True,
                text="best 0.07 (1/2) · incomplete manifest (may be transient)",
            )
        with self.assertRaises(AssertionError):
            assert_badge_shows_the_incompleteness(
                partial_read="x: Permission denied",
                partial_read_is_containment=False,
                text=(
                    "best 0.07 (1/2) · incomplete manifest "
                    "(refused: symlink or special file)"
                ),
            )
        assert_badge_shows_the_incompleteness(
            partial_read="x: symlink refused",
            partial_read_is_containment=True,
            text=(
                "best 0.07 (1/2) · incomplete manifest "
                "(refused: symlink or special file)"
            ),
        )


# ---------------------------------------------------------------------------
# The composed property issue #1086 asks for directly: for a MIX of
# readable/refused entries under ONE real folder, the explorer's per-entry
# count (``web/wrong_match_file_service.py``) and the distance path's
# per-file read (``lib/beets_distance.py``) reach the SAME verdict about
# the SAME entries. Neither module-scope test above proves this — each
# drives its OWN producer over its OWN world set, so a divergence in HOW
# MANY entries or WHICH kinds count as refused would pass both suites
# while the two surfaces of one request told the operator different
# stories about the same folder — exactly the shape of the bug this
# issue fixes (ELOOP/ENXIO counted at one end, dropped at the other).
# ---------------------------------------------------------------------------

AGREEMENT_ENTRY_KINDS: tuple[str, ...] = (
    "readable",
    "refused_eacces",
    "refused_symlink_external",
    "refused_socket",
    # Blocker 1 of the #1086 review: a symlink to a DIRECTORY, not a
    # symlink to a file. ``os.walk(followlinks=False)`` classifies it
    # into ``dirs`` and simply declines to descend — no ``onerror``
    # fires, because listing the PARENT succeeded — so this kind was
    # invisible to ``lib.beets_distance`` entirely before the walk-level
    # guard, while the explorer already refused it via
    # ``O_NOFOLLOW``/ELOOP. Without that guard this kind alone would make
    # ``distance_saw_refusal`` False while ``explorer_saw_refusal`` is
    # True — exactly the disagreement this property exists to catch.
    "symlink_to_directory",
)


def _build_agreement_world(
    album: str, kinds: Sequence[str], *, fixture_flac: str,
) -> list[str]:
    """Materialize one real folder shared by both producers.

    Every entry uses a ``.flac`` name so BOTH consumers look at the same
    population: the explorer counts every non-directory entry regardless
    of extension, but ``lib.beets_distance`` only ever considers
    ``AUDIO_EXTS`` files — a non-audio unreadable entry would make the
    two counts incomparable by construction, not by defect. Returns the
    paths a caller must ``chmod`` back before the temp dir can be removed.
    """
    locked: list[str] = []
    for index, kind in enumerate(kinds):
        path = os.path.join(album, f"{index:02d} track.flac")
        if kind == "readable":
            shutil.copy(fixture_flac, path)
        elif kind == "refused_eacces":
            shutil.copy(fixture_flac, path)
            os.chmod(path, 0o000)
            locked.append(path)
        elif kind == "refused_symlink_external":
            # The containment gap itself: a REAL, readable file outside
            # the album folder entirely.
            os.symlink(fixture_flac, path)
        elif kind == "symlink_to_directory":
            # An external, REAL directory holding a real readable track —
            # populated (not empty) so an unfixed regression shows up as
            # a silently-counted extra readable file, not merely a
            # missing refusal.
            external_dir = os.path.join(
                os.path.dirname(os.path.dirname(album)),
                f"ext-dir-{index}",
            )
            os.makedirs(external_dir, exist_ok=True)
            shutil.copy(
                fixture_flac, os.path.join(external_dir, "hidden.flac"))
            os.symlink(external_dir, path)
        else:  # refused_socket
            sock = socket.socket(socket.AF_UNIX)
            try:
                sock.bind(path)
            finally:
                sock.close()
    return locked


def assert_explorer_and_distance_agree(
    *,
    kinds: Sequence[str],
    explorer_payload: Mapping[str, object],
    distance_result: object,
) -> None:
    """The one invariant: both real producers agree about one real folder."""
    from lib.beets_distance import BeetsDistanceResult
    assert isinstance(distance_result, BeetsDistanceResult)

    readable = sum(1 for kind in kinds if kind == "readable")
    refused = len(kinds) - readable

    explorer_unreadable = explorer_payload.get("unreadable_entry_count")
    if explorer_unreadable != refused:
        raise AssertionError(
            f"kinds {list(kinds)}: explorer counted {explorer_unreadable} "
            f"unreadable entries, expected {refused}"
        )
    explorer_audio = explorer_payload.get("audio_file_count")
    if explorer_audio != readable:
        raise AssertionError(
            f"kinds {list(kinds)}: explorer listed {explorer_audio} audio "
            f"files, expected {readable} readable"
        )

    if readable > 0:
        if distance_result.outcome != "ok":
            raise AssertionError(
                f"kinds {list(kinds)}: distance reported "
                f"outcome={distance_result.outcome!r} despite {readable} "
                "readable file(s)"
            )
        if distance_result.total_local_tracks != readable:
            raise AssertionError(
                f"kinds {list(kinds)}: distance scored "
                f"{distance_result.total_local_tracks!r} local tracks, "
                f"expected the {readable} it could actually read"
            )
        if (refused > 0) != (distance_result.partial_read is not None):
            raise AssertionError(
                f"kinds {list(kinds)}: distance partial_read="
                f"{distance_result.partial_read!r} disagrees with "
                f"{refused} refused entr{'y' if refused == 1 else 'ies'}"
            )
    else:
        if distance_result.outcome != "folder_unavailable":
            raise AssertionError(
                f"kinds {list(kinds)}: every candidate was refused, "
                f"expected outcome=folder_unavailable, got "
                f"{distance_result.outcome!r}"
            )

    # The load-bearing agreement: both ends detected SOME refusal, or
    # neither did — never one seeing a refusal the other missed.
    explorer_saw_refusal = bool(explorer_unreadable)
    distance_saw_refusal = (
        distance_result.partial_read is not None
        or distance_result.outcome == "folder_unavailable"
    )
    if explorer_saw_refusal != distance_saw_refusal:
        raise AssertionError(
            f"kinds {list(kinds)}: explorer refusal={explorer_saw_refusal} "
            f"disagrees with distance refusal={distance_saw_refusal} for "
            "the SAME folder"
        )


class TestExplorerAndDistanceAgreeGenerated(unittest.TestCase):
    """Real ``build_wrong_match_explorer`` + real ``compute_beets_distance``
    over the SAME on-disk folder must reach the same counted/refused
    verdict — the widest-boundary composition issue #1086 names."""

    FIXTURE_FLAC: ClassVar[str] = os.path.join(
        os.path.dirname(__file__), "fixtures", "audio_hash", "sine_440.flac")

    @example(kinds=["readable"])
    @example(kinds=["refused_eacces"])
    @example(kinds=["refused_symlink_external"])
    @example(kinds=["refused_socket"])
    @example(kinds=["symlink_to_directory"])
    @example(kinds=["readable", "refused_eacces"])
    @example(kinds=["readable", "refused_symlink_external"])
    @example(kinds=["readable", "refused_socket"])
    @example(kinds=["readable", "symlink_to_directory"])
    @example(kinds=[
        "refused_eacces", "refused_symlink_external", "refused_socket",
        "symlink_to_directory",
    ])
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(kinds=st.lists(
        st.sampled_from(AGREEMENT_ENTRY_KINDS), min_size=1, max_size=4,
    ))
    def test_explorer_and_distance_agree_on_the_same_folder(
        self, kinds: list[str],
    ) -> None:
        from lib.beets_distance import compute_beets_distance
        from tests.test_beets_distance import _ok_mb_release, _present, _StubPDB

        db = FakePipelineDB()
        with tempfile.TemporaryDirectory() as root:
            db.seed_request(make_request_row(id=1, mb_release_id="mbid-1"))
            album = os.path.join(root, "failed_imports", "Album")
            os.makedirs(album)
            locked = _build_agreement_world(
                album, kinds, fixture_flac=self.FIXTURE_FLAC)
            log_id = db.log_download(
                1, outcome="rejected",
                validation_result={"failed_path": album},
            )
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            cfg = CratediggerConfig(
                slskd_download_dir=root,
                beets_staging_dir=os.path.join(root, "staging"),
                processing_dir=os.path.join(root, "processing"),
            )
            try:
                explorer_payload = build_wrong_match_explorer(
                    download_log_id=log_id, entry=entry, cfg=cfg,
                )
                pdb = _StubPDB(
                    download_log_entry={
                        "id": log_id, "request_id": 1,
                        "validation_result": {"failed_path": album},
                    },
                    request={"id": 1, "mb_release_group_id": "rg-shared"},
                )
                distance_result = compute_beets_distance(
                    log_id, "rel-x",
                    pdb=pdb,
                    mb_get_release=lambda mbid: _ok_mb_release(mbid=mbid),
                    observe_failed_path=_present,
                )
            finally:
                for path in locked:
                    os.chmod(path, 0o600)

        assert_explorer_and_distance_agree(
            kinds=kinds,
            explorer_payload=explorer_payload,
            distance_result=distance_result,
        )

    def test_known_bad_agreement_checker_trips(self) -> None:
        """The checker must fail on a planted DISAGREEMENT between ends."""
        from lib.beets_distance import BeetsDistanceResult

        # Explorer saw a refusal the distance path did not.
        with self.assertRaises(AssertionError):
            assert_explorer_and_distance_agree(
                kinds=["readable", "refused_eacces"],
                explorer_payload={
                    "unreadable_entry_count": 1, "audio_file_count": 1,
                },
                distance_result=BeetsDistanceResult(
                    outcome="ok", total_local_tracks=1, partial_read=None,
                ),
            )
        # Distance path saw a refusal the explorer's count missed.
        with self.assertRaises(AssertionError):
            assert_explorer_and_distance_agree(
                kinds=["readable", "refused_symlink_external"],
                explorer_payload={
                    "unreadable_entry_count": 0, "audio_file_count": 2,
                },
                distance_result=BeetsDistanceResult(
                    outcome="ok", total_local_tracks=1,
                    partial_read="02 track.flac: symlink refused",
                ),
            )
        # Every candidate refused, but distance claims a definitive
        # negative instead of the unavailable folder it actually is.
        with self.assertRaises(AssertionError):
            assert_explorer_and_distance_agree(
                kinds=["refused_socket"],
                explorer_payload={
                    "unreadable_entry_count": 1, "audio_file_count": 0,
                },
                distance_result=BeetsDistanceResult(
                    outcome="no_audio", total_local_tracks=None,
                    partial_read=None,
                ),
            )
        # Must still work: honest agreement passes both directions.
        assert_explorer_and_distance_agree(
            kinds=["readable", "refused_eacces"],
            explorer_payload={
                "unreadable_entry_count": 1, "audio_file_count": 1,
            },
            distance_result=BeetsDistanceResult(
                outcome="ok", total_local_tracks=1,
                partial_read="01 track.flac: could not be read, may be "
                "transient (EACCES)",
            ),
        )
        assert_explorer_and_distance_agree(
            kinds=["refused_socket"],
            explorer_payload={
                "unreadable_entry_count": 1, "audio_file_count": 0,
            },
            distance_result=BeetsDistanceResult(
                outcome="folder_unavailable", total_local_tracks=None,
                partial_read=None,
            ),
        )
