"""Deterministic pins for the one-album mbsync retag (#1059).

The invariants these pin — the generated siblings in
``tests/test_beets_retag_generated.py`` patrol the world space around them:

T1  The mbsync query is ANCHORED to exactly one release id. ``mbsync``
    retags everything its query matches, so an unanchored or substring
    query is the difference between one album and part of the library.
T2  A ready outcome (the caller may rekey) is returned only when the
    library is observably at the new id, or holds neither id.
T3  **mbsync's exit status is not evidence, in either direction.** It logs
    and skips a release it cannot fetch and still exits 0, so a clean exit
    without observable movement is a FAILURE; and a nonzero exit with
    observable movement is a success.
T4  Both sides held is the double-sided merge: fail closed, never retag.
    Merging or deleting either album is the operator's call (invariant 5).
T5  An unreadable or incomplete Beets authority is a failure, never
    "absent". Reading it as absence would authorize a rekey that
    manufactures a duplicate pressing.

The Beets read is driven by the repository's ``FakeBeetsDB``, whose
current-release resolver is state-derived — so the injected ``mbsync``
mutates the fake library exactly as the real command mutates the real one,
and the REAL ``retag_merged_album`` re-reads it. ``run_mbsync`` is injected,
never patched: it is a definition-time default, and patching the module
binding does not replace a captured default.
"""

from __future__ import annotations

import contextlib
import inspect
import os
import re
import sqlite3
import subprocess as sp
import tempfile
import unittest
from collections.abc import Callable, Iterator
from unittest.mock import patch

from lib.beets_db import CurrentBeetsMissing, CurrentBeetsResolution
from lib.beets_retag import (
    MBSYNC_TIMEOUT_SECONDS,
    RETAG_ALREADY_CURRENT,
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_NOT_HELD,
    RETAG_READY_OUTCOMES,
    RETAG_RETAGGED,
    MbsyncRun,
    mbsync_album_query,
    retag_merged_album,
    run_beets_mbsync,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakeBeetsDB

# The live merge probed on 2026-08-06 (request 316): the acquisition id that
# MusicBrainz merged away, and the survivor it now redirects to.
MERGED = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
SURVIVOR = "7aabf975-9a06-4b2e-854c-2c700380ebd5"

OLD = ReleaseIdentity(source="musicbrainz", release_id=MERGED)
NEW = ReleaseIdentity(source="musicbrainz", release_id=SURVIVOR)
DISCOGS = ReleaseIdentity(source="discogs", release_id="1870")


def library(
    *,
    old_album_ids: tuple[int, ...] = (),
    new_album_ids: tuple[int, ...] = (),
) -> FakeBeetsDB:
    """A fake Beets library holding the given albums under each id.

    Zero ids resolves ``missing``, one resolves ``unique``, two resolves
    ``ambiguous`` — the same cardinality semantics as the real resolver.
    """
    beets = FakeBeetsDB()
    beets.set_album_ids_for_release(MERGED, list(old_album_ids))
    beets.set_album_ids_for_release(SURVIVOR, list(new_album_ids))
    return beets


class RecordingMbsync:
    """An injected ``mbsync`` that records its query and can move the library.

    ``on_run`` stands in for the real command's effect on the Beets database;
    ``returncode`` and ``raises`` stand in for its (non-)evidence.
    """

    def __init__(
        self,
        *,
        returncode: int = 0,
        on_run: Callable[[], None] | None = None,
        raises: BaseException | None = None,
    ) -> None:
        self.returncode = returncode
        self.on_run = on_run
        self.raises = raises
        self.queries: list[str] = []

    def __call__(self, query: str) -> MbsyncRun:
        self.queries.append(query)
        if self.raises is not None:
            raise self.raises
        if self.on_run is not None:
            self.on_run()
        return MbsyncRun(returncode=self.returncode, stdout="", stderr="")


def moves_library_to_survivor(beets: FakeBeetsDB, album_id: int = 7) -> Callable[[], None]:
    """What a successful real ``mbsync`` does: the album is filed under B."""

    def apply() -> None:
        beets.set_album_ids_for_release(MERGED, [])
        beets.set_album_ids_for_release(SURVIVOR, [album_id])

    return apply


class OmittingResolver:
    """A resolver that answers for fewer identities than it was asked about."""

    def __init__(self, omit: ReleaseIdentity) -> None:
        self.omit = omit

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        return {
            identity: CurrentBeetsMissing(identity=identity)
            for identity in identities
            if identity != self.omit
        }


class RaisingResolver:
    """A resolver whose SQLite authority is unreadable.

    Rule B: the real ``BeetsDB`` reads SQLite, so the fake raises the class
    SQLite really raises, not a synthetic stand-in.
    """

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        raise sqlite3.OperationalError("database is locked")


class UnreadableAfterFirstSnapshotResolver:
    """A library that becomes unreadable after the pre-retag snapshot."""

    def __init__(self, inner: FakeBeetsDB) -> None:
        self.inner = inner
        self.snapshots = 0

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        self.snapshots += 1
        if self.snapshots > 1:
            raise sqlite3.OperationalError("database is locked")
        return self.inner.resolve_current_releases(identities)


class TestMbsyncQueryIsAnchored(unittest.TestCase):
    """T1 — the query can only ever name albums filed under exactly one id."""

    def test_query_shape(self) -> None:
        self.assertEqual(
            mbsync_album_query(OLD),
            f"mb_albumid::^{re.escape(MERGED)}$",
        )

    def test_the_regex_matches_only_the_exact_release_id(self) -> None:
        pattern = re.compile(mbsync_album_query(OLD).split("::", 1)[1])
        self.assertTrue(pattern.search(MERGED))
        for other in (
            SURVIVOR,
            MERGED[:-1],
            MERGED + "0",
            "x" + MERGED,
            MERGED.replace("-", ""),
        ):
            with self.subTest(other=other):
                self.assertIsNone(
                    pattern.search(other),
                    "an unanchored query would retag more than one album",
                )

    def test_a_non_musicbrainz_identity_is_refused(self) -> None:
        with self.assertRaises(ValueError) as caught:
            mbsync_album_query(DISCOGS)
        self.assertIn("MusicBrainz-only", str(caught.exception))


class TestReadyOutcomes(unittest.TestCase):
    def test_ready_outcomes_are_exactly_the_three_rekeyable_ones(self) -> None:
        self.assertEqual(
            RETAG_READY_OUTCOMES,
            frozenset({RETAG_RETAGGED, RETAG_ALREADY_CURRENT, RETAG_NOT_HELD}),
        )
        self.assertNotIn(RETAG_AMBIGUOUS, RETAG_READY_OUTCOMES)
        self.assertNotIn(RETAG_FAILED, RETAG_READY_OUTCOMES)


class TestRetagOutcomeBranches(unittest.TestCase):
    """One pin per branch of the real ``retag_merged_album``."""

    def test_retagged_when_the_library_observably_moves(self) -> None:
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync(on_run=moves_library_to_survivor(beets))

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_RETAGGED)
        self.assertIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(mbsync.queries, [mbsync_album_query(OLD)])
        self.assertIn(SURVIVOR, result.detail)

    def test_already_current_when_only_the_new_id_is_held(self) -> None:
        beets = library(new_album_ids=(7,))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_ALREADY_CURRENT)
        self.assertEqual(mbsync.queries, [], "nothing to retag")

    def test_not_held_when_the_library_holds_neither_id(self) -> None:
        beets = library()
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_NOT_HELD)
        self.assertEqual(mbsync.queries, [], "nothing to retag")

    def test_ambiguous_when_the_old_side_cannot_name_one_album(self) -> None:
        beets = library(old_album_ids=(7, 8))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(mbsync.queries, [])

    def test_ambiguous_when_the_new_side_cannot_name_one_album(self) -> None:
        beets = library(old_album_ids=(7,), new_album_ids=(8, 9))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertEqual(mbsync.queries, [])

    def test_both_sides_held_is_the_operators_call(self) -> None:
        """T4 — the double-sided merge. Retagging would collide two albums
        under one duplicate key; merging or deleting either is not ours."""
        beets = library(old_album_ids=(7,), new_album_ids=(8,))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(mbsync.queries, [])
        self.assertIn("7", result.detail)
        self.assertIn("8", result.detail)

    def test_identical_identities_are_refused(self) -> None:
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=OLD, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertEqual(mbsync.queries, [])

    def test_a_non_musicbrainz_identity_is_refused(self) -> None:
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            beets, old_identity=DISCOGS, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertEqual(mbsync.queries, [])


class TestMbsyncExitStatusIsNotEvidence(unittest.TestCase):
    """T3 — the decisive pair. The library decides, the subprocess does not."""

    def test_clean_exit_without_movement_is_a_failure(self) -> None:
        """``mbsync`` logs and skips a release it cannot fetch and STILL
        exits 0. Trusting that exit code would rekey the request while the
        library is still filed under the merged-away id — the exact state
        that makes the next import land a second album."""
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync(returncode=0)

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(mbsync.queries, [mbsync_album_query(OLD)])
        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertIn("did not move", result.detail)
        self.assertIn("album 7", result.detail)

    def test_nonzero_exit_with_observable_movement_is_a_success(self) -> None:
        """The converse: an exit code is not counter-evidence either."""
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync(
            returncode=1, on_run=moves_library_to_survivor(beets),
        )

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_RETAGGED)

    def test_a_raising_mbsync_without_movement_is_a_failure(self) -> None:
        beets = library(old_album_ids=(7,))
        mbsync = RecordingMbsync(
            raises=sp.TimeoutExpired(cmd=["beets", "mbsync"], timeout=120),
        )

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertIn("TimeoutExpired", result.detail)

    def test_a_partial_move_that_leaves_the_new_side_ambiguous_fails(self) -> None:
        beets = library(old_album_ids=(7,))

        def half_move() -> None:
            beets.set_album_ids_for_release(MERGED, [])
            beets.set_album_ids_for_release(SURVIVOR, [8, 9])

        result = retag_merged_album(
            beets,
            old_identity=OLD,
            new_identity=NEW,
            run_mbsync=RecordingMbsync(on_run=half_move),
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)


class TestBeetsAuthorityFailureIsNeverAbsence(unittest.TestCase):
    """T5 — an unreadable authority never authorizes a rekey."""

    def test_an_omitted_identity_is_a_failure(self) -> None:
        for omitted in (OLD, NEW):
            with self.subTest(omitted=omitted.release_id):
                mbsync = RecordingMbsync()

                result = retag_merged_album(
                    OmittingResolver(omitted),
                    old_identity=OLD,
                    new_identity=NEW,
                    run_mbsync=mbsync,
                )

                self.assertEqual(result.outcome, RETAG_FAILED)
                self.assertIn(omitted.release_id, result.detail)
                self.assertEqual(mbsync.queries, [])

    def test_an_unreadable_library_is_a_failure(self) -> None:
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            RaisingResolver(),
            old_identity=OLD,
            new_identity=NEW,
            run_mbsync=mbsync,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertIn("OperationalError", result.detail)
        self.assertEqual(mbsync.queries, [])

    def test_an_unreadable_library_after_the_retag_is_a_failure(self) -> None:
        """The post-retag re-read is the evidence; losing it is not success."""
        resolver = UnreadableAfterFirstSnapshotResolver(
            library(old_album_ids=(7,)),
        )
        mbsync = RecordingMbsync()

        result = retag_merged_album(
            resolver, old_identity=OLD, new_identity=NEW, run_mbsync=mbsync,
        )

        self.assertEqual(mbsync.queries, [mbsync_album_query(OLD)])
        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertIn("mbsync exited 0", result.detail)
        self.assertIn("OperationalError", result.detail)


class TestRunBeetsMbsyncSeam(unittest.TestCase):
    """The external edge: argv, env, and timeout wiring."""

    @contextlib.contextmanager
    def _runtime_config(self, ini_text: str) -> Iterator[None]:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "config.ini")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(ini_text)
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": path},
                clear=False,
            ):
                yield

    def test_argv_uses_the_pinned_interpreter_and_module_entry_point(self) -> None:
        """``python -m beets`` — never a ``beet`` binary from this process's
        PATH, which would be whatever beets the invoking user happens to
        have rather than the deployment-supplied runtime."""
        calls: list[tuple[list[str], dict[str, object]]] = []

        def runner(argv: list[str], **kwargs: object) -> sp.CompletedProcess[bytes]:
            calls.append((argv, kwargs))
            return sp.CompletedProcess(argv, 0, b"synced\n", b"warning\n")

        with self._runtime_config(
            "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n"
            "python = /nix/store/fake-beets/bin/python3\n"
        ):
            run = run_beets_mbsync(
                mbsync_album_query(OLD), runner=runner,
            )

        argv, kwargs = calls[0]
        self.assertEqual(argv, [
            "/nix/store/fake-beets/bin/python3",
            "-m", "beets", "mbsync",
            mbsync_album_query(OLD),
        ])
        env = kwargs["env"]
        assert isinstance(env, dict)
        self.assertEqual(env["BEETSDIR"], "/var/lib/cratedigger/beets")
        self.assertEqual(
            env["CRATEDIGGER_BEETS_PYTHON"], "/nix/store/fake-beets/bin/python3",
        )
        self.assertEqual(kwargs["timeout"], MBSYNC_TIMEOUT_SECONDS)
        self.assertIs(kwargs["capture_output"], True)
        self.assertEqual(run, MbsyncRun(0, "synced\n", "warning\n"))

    def test_an_unconfigured_interpreter_raises(self) -> None:
        def runner(argv: list[str], **kwargs: object) -> sp.CompletedProcess[bytes]:
            raise AssertionError("must not launch without a pinned interpreter")

        stripped = {
            key: value
            for key, value in os.environ.items()
            if key != "CRATEDIGGER_BEETS_PYTHON"
        }
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "config.ini")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n")
            stripped["CRATEDIGGER_RUNTIME_CONFIG"] = path
            with patch.dict(os.environ, stripped, clear=True), \
                    self.assertRaises(RuntimeError) as caught:
                run_beets_mbsync("mb_albumid::^x$", runner=runner)

        self.assertIn("CRATEDIGGER_BEETS_PYTHON", str(caught.exception))

    def test_production_wiring_is_the_captured_default(self) -> None:
        """Every test here injects ``run_mbsync``, so the one thing no test
        exercises is that production still gets the real runner. The default
        is captured at definition time — patching the module binding would
        NOT replace it — so pin the captured default directly."""
        default = inspect.signature(
            retag_merged_album,
        ).parameters["run_mbsync"].default

        self.assertIs(default, run_beets_mbsync)


if __name__ == "__main__":
    unittest.main()
