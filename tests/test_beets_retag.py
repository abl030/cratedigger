"""Deterministic pins for the one-album ``beet modify`` retag (#1059/#1087).

The invariants these pin — the generated siblings in
``tests/test_beets_retag_generated.py`` patrol the world space around them:

T1  The query is ANCHORED to exactly one release id, and the assignment
    names only the survivor. ``modify`` retags everything its query
    matches, so an unanchored or substring query is the difference between
    one album and part of the library. Query and assignment are classified
    by CONTENT, not position (``modify_parse_args``), so argv order is
    irrelevant — pinned directly.
T2  A ready outcome (the caller may rekey) is returned only when the
    library is observably at the new id, or holds neither id.
T3  **``modify``'s exit status is not evidence, in either direction.** A
    query matching NOTHING raises ``UserError`` and exits 1
    (``beets/ui/commands/utils.py::do_query`` + ``beets/ui/__init__.py``'s
    top-level handler) — not 0. The genuine exit-0-without-movement case is
    a query that MATCHES but changes nothing: ``modify_items`` prints "No
    changes to make." and returns, still exit 0. Either way, a subprocess
    exit code read against a shared SQLite file another process can
    concurrently mutate is never itself an observation of the end state —
    so a clean exit without observable movement is a FAILURE, and a
    nonzero exit with observable movement is a success.
T4  Both sides held is the double-sided merge: fail closed, never retag.
    Merging or deleting either album is the operator's call (invariant 5).
T5  An unreadable or incomplete Beets authority is a failure, never
    "absent". Reading it as absence would authorize a rekey that
    manufactures a duplicate pressing.
T6  **The real primitive moves the identity on the ALBUM row AND every
    ITEM row — this is the whole point of #1087.** #1075 DID ship a
    real-subprocess test (``TestRealMbsyncMovesIdentityNotFiles``, driving
    the real ``beet mbsync`` over four real path-shape worlds) — a real
    subprocess ran. What it never did was drive that subprocess over a
    world SHAPED LIKE the failure: its fake ``album_for_id`` returned track
    ids identical to the seeded items' ``mb_trackid``s, modelling a
    RECORDING-PRESERVING merge, while the live failure is a RELEASE-ONLY
    merge where every recording id changes. A real subprocess is
    necessary, not sufficient — the fixture must be shaped like the
    production world the invariant is about. Pinned against the REAL
    pinned Beets in ``TestRealModifyRetagMovesEveryIdentity``, including
    the exact mutant that a shape-blind test would still miss: dropping
    ``-a``, which leaves the ALBUM row behind while each ITEM's own
    ``mb_albumid`` moves — a library silently split into disagreeing
    identity fields.
T7  **An album with zero items is a real, reachable Beets state** — the
    current-release resolver's authority query is a ``LEFT JOIN`` — and
    ``resolve_current_releases`` classifies it ``CurrentBeetsAmbiguous``
    (``reason="empty_topology"``), never ``CurrentBeetsUnique``. So
    ``retag_merged_album`` refuses it exactly like any other ambiguous
    topology, BEFORE ``beet modify`` ever runs: verified empirically
    against the real resolver, not assumed (#1087 review).

Most of the Beets read is driven by the repository's ``FakeBeetsDB``, whose
current-release resolver is state-derived — so the injected ``modify``
mutates the fake library exactly as the real command mutates the real one,
and the REAL ``retag_merged_album`` re-reads it. ``run_modify`` is injected,
never patched: it is a definition-time default, and patching the module
binding does not replace a captured default. T6 and T7 are the exception:
they compose the REAL ``retag_merged_album``, the REAL
``run_beets_modify_retag`` captured default, the REAL ``beet modify``
subprocess, and the REAL ``BeetsDB`` resolver over one real temporary
library, because the invariant is about what the command does to a shared
filesystem namespace
(`.claude/rules/code-quality.md` § "Invariants live at the widest boundary").
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
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from unittest.mock import patch

import yaml
from beets import library as beets_library

from lib.beets_db import BeetsDB, CurrentBeetsMissing, CurrentBeetsResolution
from lib.beets_retag import (
    RETAG_ALREADY_CURRENT,
    RETAG_AMBIGUOUS,
    RETAG_FAILED,
    RETAG_NOT_HELD,
    RETAG_READY_OUTCOMES,
    RETAG_RETAGGED,
    RETAG_TIMEOUT_SECONDS,
    BeetsRetagResult,
    ModifyRetagRun,
    retag_album_query,
    retag_assignment,
    retag_merged_album,
    run_beets_modify_retag,
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


class RecordingModify:
    """An injected ``beet modify`` that records its args and can move the
    library.

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
        self.calls: list[tuple[str, str]] = []

    def __call__(self, query: str, assignment: str) -> ModifyRetagRun:
        self.calls.append((query, assignment))
        if self.raises is not None:
            raise self.raises
        if self.on_run is not None:
            self.on_run()
        return ModifyRetagRun(returncode=self.returncode, stdout="", stderr="")


def moves_library_to_survivor(beets: FakeBeetsDB, album_id: int = 7) -> Callable[[], None]:
    """What a successful real ``beet modify`` does: the album is filed under B."""

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


class TestRetagQueryAndAssignmentAreAnchored(unittest.TestCase):
    """T1 — the query can only ever name albums filed under exactly one id;
    the assignment can only ever carry the survivor's id."""

    def test_query_shape(self) -> None:
        self.assertEqual(
            retag_album_query(OLD),
            f"mb_albumid::^{re.escape(MERGED)}\\Z",
        )

    def test_the_regex_matches_only_the_exact_release_id(self) -> None:
        pattern = re.compile(retag_album_query(OLD).split("::", 1)[1])
        self.assertTrue(pattern.search(MERGED))
        for other in (
            SURVIVOR,
            MERGED[:-1],
            MERGED + "0",
            "x" + MERGED,
            MERGED.replace("-", ""),
            # #1087 review (F1): a bare trailing `$` also matches just
            # before ONE trailing newline in non-MULTILINE Python regex,
            # so an unrelated album whose stored id carries a stray
            # newline would match too. `\Z` (not `$`) is why this case
            # must stay rejected.
            MERGED + "\n",
        ):
            with self.subTest(other=other):
                self.assertIsNone(
                    pattern.search(other),
                    "an unanchored query would retag more than one album",
                )

    def test_a_non_musicbrainz_identity_is_refused_for_the_query(self) -> None:
        with self.assertRaises(ValueError) as caught:
            retag_album_query(DISCOGS)
        self.assertIn("MusicBrainz-only", str(caught.exception))

    def test_assignment_shape(self) -> None:
        self.assertEqual(retag_assignment(NEW), f"mb_albumid={SURVIVOR}")

    def test_a_non_musicbrainz_identity_is_refused_for_the_assignment(self) -> None:
        with self.assertRaises(ValueError) as caught:
            retag_assignment(DISCOGS)
        self.assertIn("MusicBrainz-only", str(caught.exception))

    def test_query_and_assignment_are_classified_by_content_not_position(
        self,
    ) -> None:
        """The mechanic that makes argv order irrelevant
        (``modify_parse_args``): a token is an assignment iff it contains
        ``=`` and the text before the first ``=`` contains no ``:``."""
        query = retag_album_query(OLD)
        assignment = retag_assignment(NEW)
        self.assertIn(":", query)
        self.assertNotIn("=", query)
        key, _, value = assignment.partition("=")
        self.assertNotIn(":", key)
        self.assertTrue(value)


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
        modify = RecordingModify(on_run=moves_library_to_survivor(beets))

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_RETAGGED)
        self.assertIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(
            modify.calls, [(retag_album_query(OLD), retag_assignment(NEW))],
        )
        self.assertIn(SURVIVOR, result.detail)

    def test_already_current_when_only_the_new_id_is_held(self) -> None:
        beets = library(new_album_ids=(7,))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_ALREADY_CURRENT)
        self.assertEqual(modify.calls, [], "nothing to retag")

    def test_not_held_when_the_library_holds_neither_id(self) -> None:
        beets = library()
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_NOT_HELD)
        self.assertEqual(modify.calls, [], "nothing to retag")

    def test_ambiguous_when_the_old_side_cannot_name_one_album(self) -> None:
        beets = library(old_album_ids=(7, 8))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(modify.calls, [])

    def test_ambiguous_when_the_new_side_cannot_name_one_album(self) -> None:
        beets = library(old_album_ids=(7,), new_album_ids=(8, 9))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertEqual(modify.calls, [])

    def test_both_sides_held_is_the_operators_call(self) -> None:
        """T4 — the double-sided merge. Retagging would collide two albums
        under one duplicate key; merging or deleting either is not ours."""
        beets = library(old_album_ids=(7,), new_album_ids=(8,))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_AMBIGUOUS)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(modify.calls, [])
        self.assertIn("7", result.detail)
        self.assertIn("8", result.detail)

    def test_identical_identities_are_refused(self) -> None:
        beets = library(old_album_ids=(7,))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=OLD, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertEqual(modify.calls, [])

    def test_a_non_musicbrainz_identity_is_refused(self) -> None:
        beets = library(old_album_ids=(7,))
        modify = RecordingModify()

        result = retag_merged_album(
            beets, old_identity=DISCOGS, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertEqual(modify.calls, [])


class TestModifyExitStatusIsNotEvidence(unittest.TestCase):
    """T3 — the decisive pair. The library decides, the subprocess does not."""

    def test_clean_exit_without_movement_is_a_failure(self) -> None:
        """A subprocess exit code, taken against a shared SQLite file
        another process can concurrently mutate, is never itself an
        observation of the end state — ``modify`` can exit 0 on a query
        that matched but changed nothing. Trusting that exit code would
        rekey the request while the library is still filed under the
        merged-away id — the exact state that makes the next import land a
        second album."""
        beets = library(old_album_ids=(7,))
        modify = RecordingModify(returncode=0)

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(
            modify.calls, [(retag_album_query(OLD), retag_assignment(NEW))],
        )
        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertIn("did not move", result.detail)
        self.assertIn("album 7", result.detail)

    def test_nonzero_exit_with_observable_movement_is_a_success(self) -> None:
        """The converse: an exit code is not counter-evidence either."""
        beets = library(old_album_ids=(7,))
        modify = RecordingModify(
            returncode=1, on_run=moves_library_to_survivor(beets),
        )

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_RETAGGED)

    def test_a_raising_modify_without_movement_is_a_failure(self) -> None:
        beets = library(old_album_ids=(7,))
        modify = RecordingModify(
            raises=sp.TimeoutExpired(cmd=["beets", "modify"], timeout=120),
        )

        result = retag_merged_album(
            beets, old_identity=OLD, new_identity=NEW, run_modify=modify,
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
            run_modify=RecordingModify(on_run=half_move),
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)


class TestBeetsAuthorityFailureIsNeverAbsence(unittest.TestCase):
    """T5 — an unreadable authority never authorizes a rekey."""

    def test_an_omitted_identity_is_a_failure(self) -> None:
        for omitted in (OLD, NEW):
            with self.subTest(omitted=omitted.release_id):
                modify = RecordingModify()

                result = retag_merged_album(
                    OmittingResolver(omitted),
                    old_identity=OLD,
                    new_identity=NEW,
                    run_modify=modify,
                )

                self.assertEqual(result.outcome, RETAG_FAILED)
                self.assertIn(omitted.release_id, result.detail)
                self.assertEqual(modify.calls, [])

    def test_an_unreadable_library_is_a_failure(self) -> None:
        modify = RecordingModify()

        result = retag_merged_album(
            RaisingResolver(),
            old_identity=OLD,
            new_identity=NEW,
            run_modify=modify,
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertIn("OperationalError", result.detail)
        self.assertEqual(modify.calls, [])

    def test_an_unreadable_library_after_the_retag_is_a_failure(self) -> None:
        """The post-retag re-read is the evidence; losing it is not success."""
        resolver = UnreadableAfterFirstSnapshotResolver(
            library(old_album_ids=(7,)),
        )
        modify = RecordingModify()

        result = retag_merged_album(
            resolver, old_identity=OLD, new_identity=NEW, run_modify=modify,
        )

        self.assertEqual(
            modify.calls, [(retag_album_query(OLD), retag_assignment(NEW))],
        )
        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertIn("beet modify exited 0", result.detail)
        self.assertIn("OperationalError", result.detail)


class TestRunBeetsModifyRetagSeam(unittest.TestCase):
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

    def test_argv_uses_the_pinned_interpreter_and_every_load_bearing_flag(
        self,
    ) -> None:
        """``python -m beets`` — never a ``beet`` binary from this process's
        PATH, which would be whatever beets the invoking user happens to
        have rather than the deployment-supplied runtime. Every flag is
        load-bearing; see the module docstring for why."""
        calls: list[tuple[list[str], dict[str, object]]] = []

        def runner(argv: list[str], **kwargs: object) -> sp.CompletedProcess[bytes]:
            calls.append((argv, kwargs))
            return sp.CompletedProcess(argv, 0, b"Modifying 1 albums.\n", b"")

        with self._runtime_config(
            "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n"
            "python = /nix/store/fake-beets/bin/python3\n"
        ):
            run = run_beets_modify_retag(
                retag_album_query(OLD), retag_assignment(NEW), runner=runner,
            )

        argv, kwargs = calls[0]
        self.assertEqual(argv, [
            "/nix/store/fake-beets/bin/python3",
            "-m", "beets", "modify", "-a", "-M", "-W", "-y",
            retag_album_query(OLD), retag_assignment(NEW),
        ])
        env = kwargs["env"]
        assert isinstance(env, dict)
        self.assertEqual(env["BEETSDIR"], "/var/lib/cratedigger/beets")
        self.assertEqual(
            env["CRATEDIGGER_BEETS_PYTHON"], "/nix/store/fake-beets/bin/python3",
        )
        self.assertEqual(kwargs["timeout"], RETAG_TIMEOUT_SECONDS)
        self.assertIs(kwargs["capture_output"], True)
        self.assertEqual(run, ModifyRetagRun(0, "Modifying 1 albums.\n", ""))

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
                run_beets_modify_retag(
                    "mb_albumid::^x$", "mb_albumid=y", runner=runner,
                )

        self.assertIn("CRATEDIGGER_BEETS_PYTHON", str(caught.exception))

    def test_production_wiring_is_the_captured_default(self) -> None:
        """Every test here injects ``run_modify``, so the one thing no test
        exercises is that production still gets the real runner. The default
        is captured at definition time — patching the module binding would
        NOT replace it — so pin the captured default directly."""
        default = inspect.signature(
            retag_merged_album,
        ).parameters["run_modify"].default

        self.assertIs(default, run_beets_modify_retag)


# ---------------------------------------------------------------------------
# T6 — the real command, against a real library, on a real filesystem
# ---------------------------------------------------------------------------

#: The verified-lossless sidecar. It is declared ``clutter`` in the
#: production Beets config, so a file move (which ``-M`` prevents) would
#: prune it as part of a vacated-directory cleanup.
SIDECAR_NAME = "cratedigger.json"

INSTALLED_ARTIST = "Installed Artist"
INSTALLED_ALBUM = "Installed Album"
INSTALLED_YEAR = 1999

#: The finite, CERTIFIED world space this module's generated sibling
#: patrols: not a sample, but one representative of each equivalence class
#: the primitive's own composition can actually put a real album in.
#:
#: 0  — the empty-item album. Reachable in real Beets via the
#:      current-release resolver's ``LEFT JOIN`` (``lib/beets_db.py``):
#:      ``resolve_current_releases`` classifies it ``CurrentBeetsAmbiguous``
#:      (``reason="empty_topology"``), so ``retag_merged_album`` refuses it
#:      BEFORE ``beet modify`` ever runs (T7) — a genuinely different code
#:      path from every other count, verified empirically (#1087 review).
#: 1  — the smallest world where the retag actually reaches ``modify`` and
#:      ``Album.store(inherit=True)``'s ``for item in self.items(): ...``
#:      loop (``beets/library/models.py:593-628``) iterates at all.
#: 2  — the smallest world that can prove this module's OWN per-item
#:      checks (``all(...)``/``len(...)`` over independent items) actually
#:      iterate every item rather than accidentally passing on an
#:      index-0-only check that a singleton album could never distinguish
#:      from correct. Every count above 2 repeats the identical code path
#:      per item with no cross-item interaction in either the primitive or
#:      the checker, so 2 already represents every "many" world.
#: The deterministic pin at item_count=10 (the live DICE "Midnight Zoo"
#: shape) lives outside this generated domain — see
#: ``TestRealModifyRetagMovesEveryIdentity``.
ITEM_COUNTS: tuple[int, ...] = (0, 1, 2)


def _installed_dir(root: Path) -> Path:
    return root / INSTALLED_ARTIST / f"{INSTALLED_YEAR} - {INSTALLED_ALBUM}"


def _make_real_mp3(path: Path) -> None:
    """A genuine, taggable minimal MP3 — the nix-shell environment's own
    ffmpeg (`.claude/rules/nix-shell.md`), not a synthetic byte string.
    Only the ``-W`` falsifiability test needs this; every other world in
    this module uses cheap placeholder bytes, since they never ask beets to
    write a tag."""
    sp.run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=0.08",
            "-c:a", "libmp3lame", "-b:a", "32k", str(path),
        ],
        check=True,
        capture_output=True,
    )


def _seed_real_modify_world(
    base: Path, *, item_count: int, real_audio: bool = False,
) -> tuple[Path, Path, int]:
    """Build one real Beets world: config, library DB, files.

    No plugin or metadata-source stub is needed here — unlike the
    ``mbsync`` primitive this replaces, ``beet modify`` never calls
    MusicBrainz; it needs no candidate mapping at all.

    ``item_count == 0`` builds the album row through one real seeded item,
    then deletes exactly that item's row (``with_album=False``, so the
    album row survives) and its file — beets' own ``LEFT JOIN`` current-
    release authority read proves an album row can genuinely outlive every
    one of its items, so the primitive's composition must be exercised
    against that world too, not merely assumed to handle it (T7).

    ``real_audio`` writes a genuine, taggable minimal MP3 (via ffmpeg)
    instead of placeholder bytes. Only the ``-W`` falsifiability test needs
    it — a synthetic byte string is not valid audio, so a real
    ``item.try_write()`` attempt against one fails closed with a parse
    error before touching the file, which would make a dropped ``-W``
    unfalsifiable by file mtime (#1087 review, F5a).
    """
    root = base / "library"
    root.mkdir()
    config_dir = base / "beets-config"
    config_dir.mkdir()

    album_dir = _installed_dir(root)
    album_dir.mkdir(parents=True)
    track_paths: list[Path] = []
    track_ids: list[str] = []
    seed_count = max(item_count, 1)
    for ordinal in range(1, seed_count + 1):
        track_path = album_dir / f"{ordinal:02d} Installed {ordinal}.mp3"
        if real_audio:
            _make_real_mp3(track_path)
        else:
            track_path.write_bytes(b"installed audio")
        track_paths.append(track_path)
        track_ids.append(f"{ordinal:08x}-1111-4111-8111-111111111111")
    (album_dir / SIDECAR_NAME).write_text(
        '{"verified_lossless": true}', encoding="utf-8",
    )

    library_db = base / "library.db"
    # The production path format and clutter list: identity-only movement
    # (or its absence) is observable against the real rules, not a stub.
    (config_dir / "config.yaml").write_text(
        yaml.safe_dump({
            "directory": str(root),
            "library": str(library_db),
            "plugins": "",
            "clutter": ["*.jpg", SIDECAR_NAME],
            "import": {"move": True, "copy": False, "write": True},
            "paths": {
                "default": "$albumartist/$year - $album/$track $title",
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    items = [
        beets_library.Item(
            path=str(track_path),
            title=f"Installed {ordinal}",
            artist=INSTALLED_ARTIST,
            album=INSTALLED_ALBUM,
            albumartist=INSTALLED_ARTIST,
            track=ordinal,
            disc=1,
            year=INSTALLED_YEAR,
            mb_albumid=MERGED,
            mb_trackid=track_id,
        )
        for ordinal, (track_path, track_id) in enumerate(
            zip(track_paths, track_ids, strict=True), start=1,
        )
    ]
    lib = beets_library.Library(str(library_db), str(root))
    # add_album refuses an empty item list, so item_count == 0 seeds one
    # real item to construct the album row, then deletes exactly that item.
    album = lib.add_album(items)
    if album.id is None:
        raise AssertionError("seeded Beets album is missing its database id")
    album_id = album.id
    if item_count == 0:
        for item in list(album.items()):
            item.remove(delete=False, with_album=False)
        track_paths[0].unlink()
    lib._close()

    runtime_config = base / "config.ini"
    beets_python = os.environ.get("CRATEDIGGER_BEETS_PYTHON", "")
    if not beets_python:
        raise AssertionError(
            "CRATEDIGGER_BEETS_PYTHON is unset — run under nix-shell, which "
            "supplies the admitted Beets interpreter"
        )
    runtime_config.write_text(
        "[Beets]\n"
        f"config_dir = {config_dir}\n"
        f"python = {beets_python}\n",
        encoding="utf-8",
    )
    return root, library_db, album_id


def _run_modify_without_album_flag(
    query: str, assignment: str,
) -> ModifyRetagRun:
    """The criterion-4 mutant: the exact primitive minus ``-a``.

    Run for real, against the real library — not a hand-constructed
    observation. Without ``-a``, ``modify`` targets ITEMS by default: each
    item's own ``mb_albumid`` moves, but the ALBUM row's does not.
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env["CRATEDIGGER_BEETS_PYTHON"]
    proc = sp.run(
        [
            python, "-m", "beets", "modify",
            "-M", "-W", "-y", query, assignment,
        ],
        capture_output=True,
        timeout=RETAG_TIMEOUT_SECONDS,
        env=env,
        check=False,
    )
    return ModifyRetagRun(
        returncode=proc.returncode,
        stdout=proc.stdout.decode("utf-8", errors="replace"),
        stderr=proc.stderr.decode("utf-8", errors="replace"),
    )


def _run_modify_without_nowrite_flag(
    query: str, assignment: str,
) -> ModifyRetagRun:
    """The F5a mutant: the exact primitive minus ``-W``.

    Run for real, against a real TAGGABLE library file (never the
    placeholder bytes the other fixtures use) — ``import.write: true`` in
    the fixture config matches the deployed default, so without ``-W`` this
    calls ``item.try_write()`` for real, observable as a genuine mtime
    change rather than only inferred from the argv literal.
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env["CRATEDIGGER_BEETS_PYTHON"]
    proc = sp.run(
        [
            python, "-m", "beets", "modify",
            "-a", "-M", "-y", query, assignment,
        ],
        capture_output=True,
        timeout=RETAG_TIMEOUT_SECONDS,
        env=env,
        check=False,
    )
    return ModifyRetagRun(
        returncode=proc.returncode,
        stdout=proc.stdout.decode("utf-8", errors="replace"),
        stderr=proc.stderr.decode("utf-8", errors="replace"),
    )


@dataclass(frozen=True)
class RealModifyObservation:
    """Everything the real world looked like after one real retag."""

    item_count: int
    variant: str
    result: BeetsRetagResult
    album_mb_albumid: str
    item_mb_albumids: tuple[str, ...]
    item_paths: tuple[str, ...]
    installed_dir_entries: tuple[str, ...]


@cache
def observe_real_modify_retag(
    item_count: int, *, variant: str = "correct",
) -> RealModifyObservation:
    """Run the REAL retag once per world; every caller reuses the answer."""
    with tempfile.TemporaryDirectory() as raw:
        base = Path(raw)
        root, library_db, album_id = _seed_real_modify_world(
            base, item_count=item_count,
        )
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
            clear=False,
        ), BeetsDB(str(library_db), library_root=str(root)) as beets:
            if variant == "missing_album_flag":
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                    run_modify=_run_modify_without_album_flag,
                )
            else:
                # No run_modify= — the captured production default launches
                # the real `beet modify` against the real library.
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                )

        lib = beets_library.Library(str(library_db), str(root))
        album = lib.get_album(album_id)
        if album is None:
            raise AssertionError("the seeded album vanished from the library")
        items = list(album.items())
        observation = RealModifyObservation(
            item_count=item_count,
            variant=variant,
            result=result,
            album_mb_albumid=str(album.mb_albumid),
            item_mb_albumids=tuple(str(item.mb_albumid) for item in items),
            item_paths=tuple(os.fsdecode(item.path) for item in items),
            installed_dir_entries=tuple(sorted(
                entry.name for entry in _installed_dir(root).iterdir()
            )) if _installed_dir(root).exists() else (),
        )
        lib._close()
        return observation


def check_real_modify_retag_moved_every_identity(
    observation: RealModifyObservation,
) -> None:
    """Criterion 3 (#1087) — the real primitive moves ``mb_albumid`` on the
    ALBUM row AND every ITEM row, and touches nothing else in the library.
    For ``item_count >= 1`` only — an empty-item album never reaches
    ``modify`` at all; see :func:`check_real_modify_retag_refuses_empty_topology`.

    Module level so the known-bad mutant test can call it directly — this is
    exactly the composition #1075 never exercised: every prior test injected
    the retag runner, so a real subprocess never proved it could move a real
    id over a world shaped like the failing production merge (T6).

    The relocation loop below is honest about its own limit: ``-M`` is
    belt-and-braces (see :data:`lib.beets_retag.RETAG_NOMOVE_FLAG`), not
    something this fixture's path templates make reachable — ``mb_albumid``
    names no path component, so a dropped ``-M`` mutant is NOT expected to
    trip this check; only the argv-literal seam test
    (``TestRunBeetsModifyRetagSeam``) pins ``-M``'s presence. What this loop
    DOES patrol for real: that the retag never moves a file for any OTHER
    reason on the current, real path configuration.
    """
    if observation.result.outcome != RETAG_RETAGGED:
        raise AssertionError(
            f"real modify did not retag: {observation.result!r}"
        )
    if observation.album_mb_albumid != SURVIVOR:
        raise AssertionError(
            "the ALBUM row is not filed under the survivor: "
            f"{observation.album_mb_albumid!r}"
        )
    if any(item_id != SURVIVOR for item_id in observation.item_mb_albumids):
        raise AssertionError(
            f"not every ITEM moved to the survivor: {observation.item_mb_albumids!r}"
        )
    if len(observation.item_mb_albumids) != observation.item_count:
        raise AssertionError(
            f"an item went missing during the retag: {observation.item_mb_albumids!r}"
        )
    # Compared against the FIXED expected shape, never against the observed
    # paths themselves — otherwise a world where every file relocated
    # together (all items agreeing on a new, wrong directory) would pass by
    # construction, which is exactly the shape a real relocation produces.
    for path in observation.item_paths:
        parent = Path(path).parent
        if (
            parent.name != f"{INSTALLED_YEAR} - {INSTALLED_ALBUM}"
            or parent.parent.name != INSTALLED_ARTIST
        ):
            raise AssertionError(
                f"beet modify RELOCATED an installed file to {path!r}: the "
                "retag follows an identity change, it does not reorganise "
                "the library"
            )
    if SIDECAR_NAME not in observation.installed_dir_entries:
        raise AssertionError(
            "the verified-lossless sidecar is gone from the album directory: "
            f"{observation.installed_dir_entries!r}"
        )
    if len(observation.installed_dir_entries) != observation.item_count + 1:
        raise AssertionError(
            "the album directory no longer holds exactly its tracks and the "
            f"sidecar: {observation.installed_dir_entries!r}"
        )


def check_real_modify_retag_refuses_empty_topology(
    observation: RealModifyObservation,
) -> None:
    """T7 (#1087 review) — an empty-item album is a real Beets state, and
    the composed ``retag_merged_album`` fails closed on it exactly like any
    other ambiguous topology, BEFORE ``beet modify`` ever runs.

    Module level so the known-bad self-test can call it directly.
    """
    if observation.result.outcome != RETAG_AMBIGUOUS:
        raise AssertionError(
            f"an empty-item album did not fail closed: {observation.result!r}"
        )
    if observation.album_mb_albumid != MERGED:
        raise AssertionError(
            "the ALBUM row moved despite the fail-closed outcome: "
            f"{observation.album_mb_albumid!r}"
        )
    if observation.item_mb_albumids:
        raise AssertionError(
            f"an empty-item album unexpectedly carries items: "
            f"{observation.item_mb_albumids!r}"
        )


class TestRealModifyRetagMovesEveryIdentity(unittest.TestCase):
    """T6/T7 — the real command, composed with the real guard, real
    filesystem.

    Every other test in this file stands the retag runner down to a stub
    that mutates a dict. #1075 DID ship a real-subprocess test — a real
    ``beet mbsync`` ran over four real path-shape worlds — but its fake
    metadata source modelled a RECORDING-PRESERVING merge, when the live
    failure is a RELEASE-ONLY merge where every recording id changes. A
    real subprocess is necessary, not sufficient: the fixture must be
    shaped like the failing production world.
    """

    def test_the_real_primitive_moves_the_album_and_every_item(self) -> None:
        """The live shape: the DICE "Midnight Zoo" merge (#1087) carried 10
        tracks, and ``mbsync`` moved 0 of them."""
        observation = observe_real_modify_retag(10)

        check_real_modify_retag_moved_every_identity(observation)

    def test_an_empty_item_album_fails_closed_before_modify_runs(self) -> None:
        """T7 — a real, reachable Beets state (an album row surviving its
        last item's deletion) is refused by the composed guard, never
        retagged."""
        observation = observe_real_modify_retag(0)

        check_real_modify_retag_refuses_empty_topology(observation)

    def test_dropping_the_album_flag_leaves_a_split_library(self) -> None:
        """The criterion-4 mutant. Real subprocess: drop ``-a``. ``modify``
        then targets ITEMS by default, so each item's own ``mb_albumid``
        moves while the ALBUM row's does not — a library silently split
        into disagreeing identity fields. The composed guard still refuses
        to authorize a rekey (it reads the ALBUM row via ``BeetsDB``, so it
        reports ``failed``, never ``retagged``) — but the checker that
        proves the primitive did its ONE job correctly still trips, which
        is exactly what a regression in ``-a`` should do to this test.
        """
        observation = observe_real_modify_retag(3, variant="missing_album_flag")

        with self.assertRaises(AssertionError):
            check_real_modify_retag_moved_every_identity(observation)
        self.assertNotEqual(observation.result.outcome, RETAG_RETAGGED)
        self.assertNotIn(observation.result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(observation.album_mb_albumid, MERGED)
        self.assertEqual(observation.item_mb_albumids, (SURVIVOR,) * 3)

    def test_dropping_the_nowrite_flag_writes_tags_to_the_real_file(
        self,
    ) -> None:
        """F5a (#1087 review) — ``-W`` is falsifiable by real behaviour, not
        only by the argv-literal seam pin. Drop ``-W`` for real against a
        genuinely taggable audio file: ``import.write: true`` (the fixture's
        pinned default, matching the deployed config) takes over, and
        ``modify`` calls ``item.try_write()`` on the matched item —
        observable as a changed mtime on synthetic-but-real audio, unlike
        the placeholder bytes every other test in this module uses (which a
        real write attempt fails to parse, closed, before touching the
        file — making the drop unfalsifiable by mtime against them)."""
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, _album_id = _seed_real_modify_world(
                base, item_count=1, real_audio=True,
            )
            track_path = next(_installed_dir(root).glob("*.mp3"))
            before_mtime_ns = track_path.stat().st_mtime_ns

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), BeetsDB(str(library_db), library_root=str(root)) as beets:
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                    run_modify=_run_modify_without_nowrite_flag,
                )

            after_mtime_ns = track_path.stat().st_mtime_ns

        self.assertEqual(result.outcome, RETAG_RETAGGED)
        self.assertNotEqual(
            before_mtime_ns, after_mtime_ns,
            "dropping -W left the real audio file untouched — the mutant "
            "was not killed by real behaviour",
        )


if __name__ == "__main__":
    unittest.main()
