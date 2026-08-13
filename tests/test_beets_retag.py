"""Deterministic pins for the one-album ``beet modify`` retag
(#1059/#1087/#1093).

The invariants these pin — the generated siblings in
``tests/test_beets_retag_generated.py`` patrol the world space around them:

T1  The query is an AND of two exact-match tokens — ``id:=<album_id>``,
    pinning the exact row the guard's pre-check resolved, and
    ``mb_albumid:=<old-id>``, Beets' exact-match prefix (NOT the anchored
    regex #1093 retired) and the SAME SQL-equality mechanism the post-retag
    guard already reads with — and the assignment names only the survivor.
    ``modify`` retags everything its query matches, so a query that selects
    by a DIFFERENT mechanism than the guard reads with, or by value alone
    across the time-of-check/time-of-use gap between the guard's read and
    the subprocess launch, is the gap between "the guard's belief" and
    "what actually got retagged" (#1093 item 2 + review residual). Query
    and assignment are classified by CONTENT, not position
    (``modify_parse_args``), so argv order is irrelevant — pinned directly
    against the REAL parser, not string inspection (#1093 OWED #1).
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
    ``-a``, which leaves the ALBUM row behind while ``modify`` targets
    ITEMS by default instead — since #1093's compound query, this means
    the ``id:=<album_id>`` clause now binds to the wrong table's OWN
    primary key namespace, so only an item whose own id coincidentally
    equals the album's id moves at all, live-verified as a deterministic
    single-item collision in this fixture (not every item, as the
    value-only query would have moved) — still a library silently split
    into disagreeing identity fields, just via a different mechanism.
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
from beets.dbcore.query import CollectionQuery, MatchQuery, RegexpQuery
from beets.library.queries import parse_query_parts
from beets.ui.commands.modify import modify_parse_args

from lib.beets_db import (
    BeetsDB,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
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
    RetagModifyFn,
    retag_album_query,
    retag_assignment,
    retag_merged_album,
    run_beets_modify_retag,
)
from lib.release_identity import ReleaseIdentity
from tests.beets_world import extract_consumer_beets_world_config
from tests.fakes import FakeBeetsDB

REPO_ROOT = Path(__file__).resolve().parent.parent

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
        self.calls: list[tuple[tuple[str, str], str]] = []

    def __call__(
        self, query_tokens: tuple[str, str], assignment: str,
    ) -> ModifyRetagRun:
        self.calls.append((query_tokens, assignment))
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


class TestRetagQueryAndAssignmentSelectExactly(unittest.TestCase):
    """T1 (#1093 item 2 + review residual) — the query is an AND of two
    exact-match tokens: ``id:=<album_id>`` pins the exact row the guard's
    pre-check resolved, and ``mb_albumid:=<old-id>`` keeps the identity
    compare-and-set, using the SAME mechanism the post-retag guard reads
    with. The assignment can only ever carry the survivor's id. Verified
    against the REAL Beets query parser and command-argument classifier —
    a source read is not evidence for a load-bearing claim in this repo
    (OWED #1, `.claude/rules/test-fidelity.md` Rule C)."""

    def test_query_shape(self) -> None:
        self.assertEqual(
            retag_album_query(OLD, album_id=7), ("id:=7", f"mb_albumid:={MERGED}"),
        )

    def test_the_query_parses_to_an_exact_match_not_a_regex(self) -> None:
        """Live-verified against the real Beets query parser
        (``beets/library/queries.py::parse_query_parts``): both tokens map
        to ``dbcore.query.MatchQuery`` — exact SQL equality — never
        ``RegexpQuery``, and Beets ANDs them. This is the mechanism fix at
        the heart of #1093 item 2: the post-retag guard (``lib/beets_db.py::
        BeetsDB.resolve_current_releases``) already selects by exact SQL
        equality, so the retag's own query must use the identical
        mechanism rather than an independently-correct one that can
        disagree on a value the guard cannot see (a BLOB-stored
        ``mb_albumid``, only reachable via a third-party raw-SQL writer —
        see ``TestExactMatchQueryConvergesWithTheGuard`` below)."""
        id_token, mb_albumid_token = retag_album_query(OLD, album_id=7)
        query, _sort = parse_query_parts(
            [id_token, mb_albumid_token], beets_library.Album,
        )
        assert isinstance(query, CollectionQuery)  # narrow for pyright
        match_queries = [
            subquery for subquery in query.subqueries
            if isinstance(subquery, MatchQuery)
        ]
        regexp_queries = [
            subquery for subquery in query.subqueries
            if isinstance(subquery, RegexpQuery)
        ]
        self.assertEqual(regexp_queries, [], "must not compile to a regex query")
        self.assertEqual(len(match_queries), 2)
        fields = {mq.field_name: mq for mq in match_queries}
        self.assertEqual(set(fields), {"id", "mb_albumid"})
        self.assertEqual(fields["id"].pattern, "7")
        self.assertEqual(fields["mb_albumid"].pattern, MERGED)
        self.assertEqual(
            query.clause(),
            ("(albums.id = ?) and (albums.mb_albumid = ?)", ["7", MERGED]),
            "must be plain SQL equality on BOTH clauses — the same shape "
            "resolve_current_releases already reads with",
        )

        # The retired form, for contrast only — never a live code path
        # anymore (`retag_album_query` cannot build this shape). Proves the
        # two forms really do parse to different query classes, so the
        # mechanism claim above is not merely asserted.
        retired_query, _sort2 = parse_query_parts(
            [f"mb_albumid::^{re.escape(MERGED)}\\Z"], beets_library.Album,
        )
        assert isinstance(retired_query, CollectionQuery)  # narrow for pyright
        retired_regexp_queries = [
            subquery for subquery in retired_query.subqueries
            if isinstance(subquery, RegexpQuery)
        ]
        self.assertEqual(len(retired_regexp_queries), 1)

    def test_a_non_musicbrainz_identity_is_refused_for_the_query(self) -> None:
        with self.assertRaises(ValueError) as caught:
            retag_album_query(DISCOGS, album_id=7)
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
        """Live-verified against the REAL ``modify_parse_args``
        (``beets/ui/commands/modify.py``): both query tokens (containing
        BOTH ``:`` and ``=`` since #1093 — each colon always precedes its
        ``=``) are always classified as QUERY tokens, and the assignment
        (``=`` with no leading ``:``) is always classified as an
        ASSIGNMENT, for every argv ordering."""
        id_token, mb_albumid_token = retag_album_query(OLD, album_id=7)
        assignment = retag_assignment(NEW)
        orderings = (
            [id_token, mb_albumid_token, assignment],
            [assignment, mb_albumid_token, id_token],
            [mb_albumid_token, assignment, id_token],
        )
        for args in orderings:
            with self.subTest(args=args):
                parsed_query, mods, dels = modify_parse_args(args, is_album=True)
                self.assertEqual(set(parsed_query), {id_token, mb_albumid_token})
                self.assertEqual(dels, [])
                self.assertIn("mb_albumid", mods)
                self.assertEqual(mods["mb_albumid"].value, SURVIVOR)


def _seed_two_album_world(
    base: Path, *, phantom_identity: str, phantom_as_blob: bool,
) -> tuple[Path, Path, int, int]:
    """Two real albums: a genuine target at MERGED (plain TEXT
    ``mb_albumid``, written by Beets itself), and a second, otherwise
    unrelated "phantom" whose ``mb_albumid`` a raw third-party write
    overwrites to ``phantom_identity`` — as a genuine SQLite BLOB when
    ``phantom_as_blob``, otherwise as plain TEXT (#1093 item 2). Only the
    ALBUM row is touched: ``-a`` targets the ``albums`` table, so the
    phantom's ITEM row is irrelevant to what ``modify -a`` can select.

    Two callers use this: a same-string BLOB phantom (unreachable through
    Beets itself — Beets always writes ``str`` — the shape a raw-SQL writer
    could produce) and a same-PREFIX plain-TEXT decoy (``MERGED + "0"``,
    which a substring/prefix-matching query would also select but exact
    SQL equality cannot).
    """
    root = base / "library"
    root.mkdir()
    config_dir = base / "beets-config"
    config_dir.mkdir()
    library_db = base / "library.db"
    (config_dir / "config.yaml").write_text(
        yaml.safe_dump({
            "directory": str(root),
            "library": str(library_db),
            "plugins": "",
            "import": {"move": True, "copy": False, "write": True},
            "paths": {"default": "$albumartist/$year - $album/$track $title"},
        }, sort_keys=False),
        encoding="utf-8",
    )

    def make_item(artist: str, ident: str, track_id: str) -> beets_library.Item:
        album_dir = root / artist / "1999 - Album"
        album_dir.mkdir(parents=True)
        track_path = album_dir / "01 Track.mp3"
        track_path.write_bytes(b"fake audio")
        return beets_library.Item(
            path=str(track_path), title="Track", artist=artist, album="Album",
            albumartist=artist, track=1, disc=1, year=1999,
            mb_albumid=ident, mb_trackid=track_id,
        )

    lib = beets_library.Library(str(library_db), str(root))
    target_album = lib.add_album([make_item(
        "Target Artist", MERGED, "00000000-1111-4111-8111-111111111111",
    )])
    phantom_album = lib.add_album([make_item(
        "Phantom Artist", MERGED, "00000001-1111-4111-8111-111111111111",
    )])
    if target_album.id is None or phantom_album.id is None:
        raise AssertionError("seeded Beets album is missing its database id")
    target_id, phantom_id = target_album.id, phantom_album.id
    lib._close()

    conn = sqlite3.connect(str(library_db))
    stored_value: memoryview[int] | str = (
        sqlite3.Binary(phantom_identity.encode("utf-8"))
        if phantom_as_blob else phantom_identity
    )
    conn.execute(
        "UPDATE albums SET mb_albumid = ? WHERE id = ?",
        (stored_value, phantom_id),
    )
    conn.commit()
    conn.close()

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
    return root, library_db, target_id, phantom_id


class TestExactMatchQueryConvergesWithTheGuard(unittest.TestCase):
    """#1093 item 2 — one selection mechanism, proven by composing the REAL
    exact-match query, a REAL two-album library, and the REAL guard
    (``lib.beets_db.BeetsDB``). Two divergence shapes, both real: a
    BLOB-stored phantom (before this fix, the anchored regex — evaluated by
    Beets' ``regexp()`` UDF, which decodes bytes — could see it while the
    guard's exact SQL equality could not) and a same-PREFIX plain-TEXT
    decoy (which a substring/prefix-matching query would also select).
    This proves AGREEMENT, not merely coverage: both phantoms are now
    invisible to BOTH sides, and a real ``beet modify`` run through the
    composed production entry point touches only the album the guard
    already counted.
    """

    def test_a_blob_stored_phantom_is_invisible_to_the_guard_and_the_query(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, target_id, phantom_id = _seed_two_album_world(
                base, phantom_identity=MERGED, phantom_as_blob=True,
            )

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), BeetsDB(str(library_db), library_root=str(root)) as beets:
                guard_before = beets.resolve_current_release(OLD)
                self.assertIsInstance(guard_before, CurrentBeetsUnique)
                assert isinstance(guard_before, CurrentBeetsUnique)  # narrow for pyright
                self.assertEqual(guard_before.album_id, target_id)

                # The composed REAL retag: real guard, real captured
                # `run_beets_modify_retag` default, real subprocess.
                result = retag_merged_album(beets, old_identity=OLD, new_identity=NEW)

            self.assertEqual(result.outcome, RETAG_RETAGGED)
            self.assertIn(str(target_id), result.detail)

            conn = sqlite3.connect(str(library_db))
            target_row = conn.execute(
                "SELECT mb_albumid, typeof(mb_albumid) FROM albums WHERE id = ?",
                (target_id,),
            ).fetchone()
            phantom_row = conn.execute(
                "SELECT mb_albumid, typeof(mb_albumid) FROM albums WHERE id = ?",
                (phantom_id,),
            ).fetchone()
            conn.close()

            self.assertEqual(target_row, (SURVIVOR, "text"))
            self.assertEqual(
                phantom_row, (MERGED.encode("utf-8"), "blob"),
                "the phantom must stay untouched — the exact-match query must "
                "not select it",
            )

    def test_the_retired_regex_form_would_have_matched_the_phantom_too(
        self,
    ) -> None:
        """Historical evidence only (#1093 review; test-fidelity.md Rule
        C) — not a live code path, since :func:`retag_album_query` no
        longer knows how to build this shape. Runs the retired query
        string directly as a real ``beet modify`` subprocess against the
        SAME two-album world, proving the regex form's divergence from the
        guard was real, not hypothetical: live-verified, it retagged BOTH
        albums. Asserted against the OBSERVABLE END STATE (the re-read
        database), never a copy of Beets' stdout — a Beets copy change
        would break the pin without the underlying behavior changing
        (#1093 review F5)."""
        from lib.util import beets_subprocess_env

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            _root, library_db, target_id, phantom_id = _seed_two_album_world(
                base, phantom_identity=MERGED, phantom_as_blob=True,
            )

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ):
                env = beets_subprocess_env()
            retired_query = f"mb_albumid::^{re.escape(MERGED)}\\Z"
            proc = sp.run(
                [
                    env["CRATEDIGGER_BEETS_PYTHON"], "-m", "beets", "modify",
                    "-a", "-M", "-W", "-y", retired_query, f"mb_albumid={SURVIVOR}",
                ],
                capture_output=True, env=env, timeout=RETAG_TIMEOUT_SECONDS,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr.decode())

            conn = sqlite3.connect(str(library_db))
            rows = conn.execute(
                "SELECT id, mb_albumid FROM albums ORDER BY id",
            ).fetchall()
            conn.close()
            moved_ids = {row[0] for row in rows if row[1] == SURVIVOR}
            self.assertEqual(
                moved_ids, {target_id, phantom_id},
                "the retired regex form silently retagged the phantom too — "
                "exactly the divergence #1093 fixed",
            )

    def test_the_query_selects_only_the_exact_target_not_a_same_prefix_decoy(
        self,
    ) -> None:
        """The complementary case to the BLOB phantom above: a decoy whose
        id is ``MERGED + "0"`` — a plain TEXT value, written the ordinary
        way, that a substring/prefix-matching query would also select.
        Exact SQL equality cannot: ``albums.mb_albumid = ?`` bound to
        MERGED never matches a value one character longer."""
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, target_id, decoy_id = _seed_two_album_world(
                base, phantom_identity=MERGED + "0", phantom_as_blob=False,
            )

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), BeetsDB(str(library_db), library_root=str(root)) as beets:
                result = retag_merged_album(beets, old_identity=OLD, new_identity=NEW)

            self.assertEqual(result.outcome, RETAG_RETAGGED)

            conn = sqlite3.connect(str(library_db))
            target_row = conn.execute(
                "SELECT mb_albumid FROM albums WHERE id = ?", (target_id,),
            ).fetchone()
            decoy_row = conn.execute(
                "SELECT mb_albumid FROM albums WHERE id = ?", (decoy_id,),
            ).fetchone()
            conn.close()

            self.assertEqual(target_row, (SURVIVOR,))
            self.assertEqual(
                decoy_row, (MERGED + "0",),
                "the same-prefix decoy must stay untouched — the "
                "exact-match query is not a prefix or substring match",
            )


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
            modify.calls, [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
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
            modify.calls, [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
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
        """#1093 item 5 — this is the exact world that produced a
        self-contradictory failure detail: the old id genuinely moved away
        (``not held``), yet the pre-fix message unconditionally claimed
        "the library did not move" regardless. The detail must tell the
        truth about which side actually changed."""
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
        self.assertNotIn(
            "did not move", result.detail,
            "the old id observably moved away (not_held) — claiming "
            '"did not move" while that is true is self-contradictory',
        )
        self.assertIn("moved off", result.detail)
        self.assertIn(MERGED, result.detail)
        self.assertIn("ambiguous", result.detail)
        self.assertIn("8", result.detail)
        self.assertIn("9", result.detail)

    def test_a_concurrent_second_album_at_the_old_id_is_never_moved_off(
        self,
    ) -> None:
        """#1093 review round 2 (F1) — the reviewer's own counterexample:
        a concurrent writer files a SECOND album at the old id while
        ``modify`` itself moves nothing. ``old_after`` is Ambiguous — still
        held, by MORE albums than before — never "moved off": every
        Ambiguous reason requires at least one matching album row."""
        beets = library(old_album_ids=(7,))

        def concurrent_writer_lands_a_second_album() -> None:
            beets.set_album_ids_for_release(MERGED, [7, 9])

        result = retag_merged_album(
            beets,
            old_identity=OLD,
            new_identity=NEW,
            run_modify=RecordingModify(
                returncode=1, on_run=concurrent_writer_lands_a_second_album,
            ),
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertNotIn(
            "moved off", result.detail,
            "the old id is STILL held (by two albums now) — claiming "
            '"moved off" while that is true is self-contradictory',
        )
        self.assertIn("still held but", result.detail)
        self.assertIn(MERGED, result.detail)
        self.assertIn("7", result.detail)
        self.assertIn("9", result.detail)

    def test_a_different_album_occupying_the_old_id_afterward_is_a_changed_occupant(
        self,
    ) -> None:
        """#1093 review round 2 sub-point — the ORIGINAL album (7) vanishes
        from the old id while a DIFFERENT album (9) lands there instead
        (e.g. a concurrent out-of-band retag this execution did not cause).
        Neither "did not move" (album 7 IS gone from the old id) nor
        "moved off" (the old id IS still held, just by album 9 now)."""
        beets = library(old_album_ids=(7,))

        def different_album_takes_the_old_id() -> None:
            beets.set_album_ids_for_release(MERGED, [9])

        result = retag_merged_album(
            beets,
            old_identity=OLD,
            new_identity=NEW,
            run_modify=RecordingModify(on_run=different_album_takes_the_old_id),
        )

        self.assertEqual(result.outcome, RETAG_FAILED)
        self.assertNotIn(result.outcome, RETAG_READY_OUTCOMES)
        self.assertNotIn("did not move", result.detail)
        self.assertNotIn("moved off", result.detail)
        self.assertIn("changed occupant", result.detail)
        self.assertIn("album 7", result.detail)
        self.assertIn("album 9", result.detail)


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
            modify.calls, [(retag_album_query(OLD, album_id=7), retag_assignment(NEW))],
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

        query_tokens = retag_album_query(OLD, album_id=7)
        with self._runtime_config(
            "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n"
            "python = /nix/store/fake-beets/bin/python3\n"
        ):
            run = run_beets_modify_retag(
                query_tokens, retag_assignment(NEW), runner=runner,
            )

        argv, kwargs = calls[0]
        self.assertEqual(argv, [
            "/nix/store/fake-beets/bin/python3",
            "-m", "beets", "modify", "-a", "-M", "-W", "-y",
            *query_tokens, retag_assignment(NEW),
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
                    ("id:=1", "mb_albumid:=x"), "mb_albumid=y", runner=runner,
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
#: The album directory name under the CURRENT production path template
#: (``$albumartist/$year - $album/...``, no ``$mb_albumid``), which is why
#: ``check_real_modify_retag_moved_every_identity``'s relocation/sidecar
#: clauses default to it.
INSTALLED_ALBUM_DIR_NAME = f"{INSTALLED_YEAR} - {INSTALLED_ALBUM}"

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
    return root / INSTALLED_ARTIST / INSTALLED_ALBUM_DIR_NAME


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
    plugins: tuple[str, ...] = (),
) -> tuple[Path, Path, int]:
    """Build one real Beets world: config, library DB, files.

    No plugin or metadata-source stub is needed here — unlike the
    ``mbsync`` primitive this replaces, ``beet modify`` never calls
    MusicBrainz; it needs no candidate mapping at all. ``plugins`` defaults
    to none loaded; passing the deployment list proves the primitive
    behaves identically under the real plugin world, not merely that the
    plugins load (#1093 item 3).

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
    config: dict[str, object] = {
        "directory": str(root),
        "library": str(library_db),
        "plugins": " ".join(plugins),
        "clutter": ["*.jpg", SIDECAR_NAME],
        "import": {"move": True, "copy": False, "write": True},
        "paths": {
            "default": "$albumartist/$year - $album/$track $title",
        },
    }
    if "discogs" in plugins:
        # Live-verified (#1093 item 3): without a token the discogs plugin
        # fails to LOAD (`setup()` tries an interactive OAuth prompt and
        # raises `UserError` on EOF) — non-fatal to `beet modify` itself
        # (beets logs "error loading plugin discogs" and continues), but a
        # bogus `user_token` avoids that noise and matches production,
        # which always has a real token provisioned (CLAUDE.md § Secrets).
        config["discogs"] = {"user_token": "test-token"}
    (config_dir / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8",
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
    query_tokens: tuple[str, str], assignment: str,
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
            "-M", "-W", "-y", *query_tokens, assignment,
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
    query_tokens: tuple[str, str], assignment: str,
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
            "-a", "-M", "-y", *query_tokens, assignment,
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


def _run_modify_without_nomove_flag(
    query_tokens: tuple[str, str], assignment: str,
) -> ModifyRetagRun:
    """The item-4 mutant (#1093): the exact primitive minus ``-M``.

    Run for real, against a real library whose path template makes
    ``mb_albumid`` path-relevant — only under such a template is the
    relocation this drops genuinely reachable; the CURRENT production
    template makes it unfalsifiable (see
    ``TestRealModifyRetagRelocationAndSidecarClausesAreReachable``).
    """
    from lib.util import beets_subprocess_env

    env = beets_subprocess_env()
    python = env["CRATEDIGGER_BEETS_PYTHON"]
    proc = sp.run(
        [
            python, "-m", "beets", "modify",
            "-a", "-W", "-y", *query_tokens, assignment,
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
    #: F2 (#1087 review) — the must-still-work counterpart to the ``-W``
    #: mutant test: captured before and after the retag, sorted by path so
    #: the two tuples line up positionally. The production primitive (``-W``
    #: present) never attempts a write, so these must be pairwise equal —
    #: proven on real files, not inferred from the argv literal.
    item_mtimes_before_ns: tuple[int, ...]
    item_mtimes_after_ns: tuple[int, ...]


def _track_files(root: Path) -> list[Path]:
    return (
        sorted(_installed_dir(root).glob("*.mp3"))
        if _installed_dir(root).exists() else []
    )


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
        track_files = _track_files(root)
        item_mtimes_before_ns = tuple(path.stat().st_mtime_ns for path in track_files)
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
        item_mtimes_after_ns = tuple(path.stat().st_mtime_ns for path in track_files)

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
            item_mtimes_before_ns=item_mtimes_before_ns,
            item_mtimes_after_ns=item_mtimes_after_ns,
        )
        lib._close()
        return observation


def check_real_modify_retag_moved_every_identity(
    observation: RealModifyObservation,
    *,
    expected_artist: str = INSTALLED_ARTIST,
    expected_album_dir_name: str = INSTALLED_ALBUM_DIR_NAME,
) -> None:
    """Criterion 3 (#1087) — the real primitive moves ``mb_albumid`` on the
    ALBUM row AND every ITEM row, touches no file's mtime, and touches
    nothing else in the library. For ``item_count >= 1`` only — an
    empty-item album never reaches ``modify`` at all; see
    :func:`check_real_modify_retag_refuses_empty_topology`.

    The mtime assertion is the must-still-work counterpart to
    ``test_dropping_the_nowrite_flag_writes_tags_to_the_real_file``: that
    test proves dropping ``-W`` writes a real file; this proves the
    production primitive (``-W`` present) does not.

    Module level so the known-bad mutant test can call it directly — this is
    exactly the composition #1075 never exercised: its real-subprocess test
    (``TestRealMbsyncMovesIdentityNotFiles``) ran over a world shaped like a
    RECORDING-PRESERVING merge, so it never proved the primitive could move
    an id over a world shaped like the RELEASE-ONLY merge that actually
    occurs in production (T6).

    ``expected_artist``/``expected_album_dir_name`` default to the CURRENT
    production path template's shape (``$albumartist/$year - $album/...``,
    no ``$mb_albumid``), under which a dropped ``-M`` cannot relocate
    anything — every T6/T7 caller below relies on that default and is
    unaffected by this parameter's existence. Passing a different pair lets
    :class:`TestRealModifyRetagRelocationAndSidecarClausesAreReachable`
    reuse this SAME relocation/sidecar clause logic against a fixture world
    whose path template DOES include ``$mb_albumid`` — where the relocation
    and sidecar-prune clauses are genuinely reachable and killable by a real
    ``-M``-dropped mutant, closing the #1093 item-4 gap: before this, both
    clauses could never fail against ANY world this module's fixtures could
    produce.
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
    # F2 (#1087 review) — the must-still-work control for the -W mutant
    # test: the production primitive never attempts a write, so no file's
    # mtime may move. Proven on real files, the same way the -W-dropped
    # mutant proves the converse.
    if observation.item_mtimes_after_ns != observation.item_mtimes_before_ns:
        raise AssertionError(
            "beet modify WROTE to a real file despite -W: "
            f"before={observation.item_mtimes_before_ns!r} "
            f"after={observation.item_mtimes_after_ns!r}"
        )
    # Compared against the FIXED expected shape, never against the observed
    # paths themselves — otherwise a world where every file relocated
    # together (all items agreeing on a new, wrong directory) would pass by
    # construction, which is exactly the shape a real relocation produces.
    for path in observation.item_paths:
        parent = Path(path).parent
        if (
            parent.name != expected_album_dir_name
            or parent.parent.name != expected_artist
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
        then targets ITEMS by default, so the ``id:=<album_id>`` clause
        (#1093 review residual) no longer pins the album row the guard
        resolved — it matches whichever ITEM happens to share that primary
        key in the ITEMS table's own INDEPENDENT id sequence, rather than
        being ignored. Live-verified, deterministic in this fixture: SQLite
        autoincrement starts each table's sequence at 1 independently, and
        this fixture seeds exactly one fresh album with N items in a brand
        new library file, so the album's id is always 1 and the FIRST
        item's id is always 1 too — coincidental overlap, not by value.
        Only that one item's ``mb_albumid`` moves; the other two (which DO
        match by value, just not by this coincidental id) and the ALBUM
        row's do not — a library silently split into disagreeing identity
        fields, with the compound query making an UNRELATED item's
        coincidental primary key the deciding factor rather than the
        query simply degrading to value-only matching. The composed guard
        still refuses to authorize a rekey (it reads the ALBUM row via
        ``BeetsDB``, so it reports ``failed``, never ``retagged``) — but
        the checker that proves the primitive did its ONE job correctly
        still trips, which is exactly what a regression in ``-a`` should
        do to this test.
        """
        observation = observe_real_modify_retag(3, variant="missing_album_flag")

        with self.assertRaises(AssertionError):
            check_real_modify_retag_moved_every_identity(observation)
        self.assertNotEqual(observation.result.outcome, RETAG_RETAGGED)
        self.assertNotIn(observation.result.outcome, RETAG_READY_OUTCOMES)
        self.assertEqual(observation.album_mb_albumid, MERGED)
        self.assertEqual(
            observation.item_mb_albumids, (SURVIVOR, MERGED, MERGED),
            "only the item whose OWN id coincidentally equals the album's "
            "id should move — proving -a's absence lets the id clause "
            "bind to the wrong table's primary key namespace",
        )

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


# ---------------------------------------------------------------------------
# #1093 item 4 — the relocation/sidecar clauses, made genuinely reachable
# ---------------------------------------------------------------------------

def _identity_relevant_album_dir(root: Path, release_id: str) -> Path:
    """The album directory for the path template used by
    :func:`_seed_identity_relevant_path_world`, which — unlike every other
    fixture in this module — makes ``mb_albumid`` a path component."""
    return root / INSTALLED_ARTIST / f"{INSTALLED_ALBUM_DIR_NAME} [{release_id}]"


def _seed_identity_relevant_path_world(base: Path) -> tuple[Path, Path, int]:
    """One real Beets world whose path template embeds ``$mb_albumid`` —
    unlike every OTHER fixture in this module, where identity names no path
    component, so a dropped ``-M`` cannot relocate anything and the
    relocation/sidecar-prune clauses in
    :func:`check_real_modify_retag_moved_every_identity` are permanently
    unreachable by any world those fixtures can produce.

    Live-verified (#1093 item 4): under THIS template, dropping ``-M``
    genuinely moves the track file into the new-id-named directory, and the
    vacated old directory's clutter sweep genuinely removes the sidecar
    (``prune_dirs``) — turning both previously-unfalsifiable clauses into
    ones a real mutant can kill.
    """
    root = base / "library"
    root.mkdir()
    config_dir = base / "beets-config"
    config_dir.mkdir()
    library_db = base / "library.db"

    album_dir = _identity_relevant_album_dir(root, MERGED)
    album_dir.mkdir(parents=True)
    track_path = album_dir / "01 Installed 1.mp3"
    track_path.write_bytes(b"installed audio")
    (album_dir / SIDECAR_NAME).write_text(
        '{"verified_lossless": true}', encoding="utf-8",
    )

    (config_dir / "config.yaml").write_text(
        yaml.safe_dump({
            "directory": str(root),
            "library": str(library_db),
            "plugins": "",
            "clutter": ["*.jpg", SIDECAR_NAME],
            "import": {"move": True, "copy": False, "write": True},
            "paths": {
                "default": (
                    "$albumartist/$year - $album [$mb_albumid]/$track $title"
                ),
            },
        }, sort_keys=False),
        encoding="utf-8",
    )

    item = beets_library.Item(
        path=str(track_path), title="Installed 1", artist=INSTALLED_ARTIST,
        album=INSTALLED_ALBUM, albumartist=INSTALLED_ARTIST, track=1, disc=1,
        year=INSTALLED_YEAR, mb_albumid=MERGED,
        mb_trackid="00000001-1111-4111-8111-111111111111",
    )
    lib = beets_library.Library(str(library_db), str(root))
    album = lib.add_album([item])
    if album.id is None:
        raise AssertionError("seeded Beets album is missing its database id")
    album_id = album.id
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


def _observe_identity_relevant_path_retag(
    *, run_modify: RetagModifyFn | None,
) -> RealModifyObservation:
    """Run one real retag against the identity-relevant-path world. Not
    memoised (unlike :func:`observe_real_modify_retag`) — each caller below
    needs its own fresh world, and there are only two of them."""
    with tempfile.TemporaryDirectory() as raw:
        base = Path(raw)
        root, library_db, album_id = _seed_identity_relevant_path_world(base)
        old_dir = _identity_relevant_album_dir(root, MERGED)
        track_path = next(old_dir.glob("*.mp3"))
        before_mtime_ns = track_path.stat().st_mtime_ns

        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
            clear=False,
        ), BeetsDB(str(library_db), library_root=str(root)) as beets:
            if run_modify is None:
                # No run_modify= — the captured production default.
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                )
            else:
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                    run_modify=run_modify,
                )

        lib = beets_library.Library(str(library_db), str(root))
        album = lib.get_album(album_id)
        if album is None:
            raise AssertionError("the seeded album vanished from the library")
        items = list(album.items())
        item_paths = tuple(os.fsdecode(item.path) for item in items)
        after_mtime_ns = tuple(
            Path(path).stat().st_mtime_ns for path in item_paths
        )
        current_dirs = {Path(path).parent for path in item_paths}
        current_dir = next(iter(current_dirs)) if len(current_dirs) == 1 else None
        entries = (
            tuple(sorted(entry.name for entry in current_dir.iterdir()))
            if current_dir is not None and current_dir.exists() else ()
        )
        lib._close()
        return RealModifyObservation(
            item_count=1,
            variant="identity_relevant_path",
            result=result,
            album_mb_albumid=str(album.mb_albumid),
            item_mb_albumids=tuple(str(item.mb_albumid) for item in items),
            item_paths=item_paths,
            installed_dir_entries=entries,
            item_mtimes_before_ns=(before_mtime_ns,),
            item_mtimes_after_ns=after_mtime_ns,
        )


class TestRealModifyRetagRelocationAndSidecarClausesAreReachable(
    unittest.TestCase,
):
    """#1093 item 4 — ``check_real_modify_retag_moved_every_identity``'s
    relocation and sidecar-prune clauses cannot fail against ANY world
    ``_seed_real_modify_world`` can produce, because its path template
    never makes ``mb_albumid`` path-relevant. Per
    `.claude/rules/code-quality.md` § "the remedy for a survivor is to
    widen the strategy, not delete the clause": this widens the fixture
    world (:func:`_seed_identity_relevant_path_world`) rather than deleting
    either clause, and plants the real ``-M``-dropped mutant to prove both
    are now genuinely killable — closing the gap while ALSO empirically
    proving :data:`lib.beets_retag.RETAG_NOMOVE_FLAG`'s own docstring claim
    for the first time, rather than merely asserting it.
    """

    def test_the_production_primitive_never_relocates_even_when_identity_is_path_relevant(
        self,
    ) -> None:
        """Must-still-work: with ``-M`` present (the real captured
        default), the file stays exactly where it was — still named for
        the MERGED id — even though the path template WOULD make the new
        id a different directory."""
        observation = _observe_identity_relevant_path_retag(run_modify=None)

        self.assertEqual(observation.result.outcome, RETAG_RETAGGED)
        check_real_modify_retag_moved_every_identity(
            observation,
            expected_artist=INSTALLED_ARTIST,
            expected_album_dir_name=f"{INSTALLED_ALBUM_DIR_NAME} [{MERGED}]",
        )

    def test_dropping_nomove_relocates_the_file_for_real(self) -> None:
        """The item-4 mutant kills the RELOCATION clause: told the file
        should still be at the OLD (MERGED-named) directory, the checker
        must catch that it moved to the NEW one."""
        observation = _observe_identity_relevant_path_retag(
            run_modify=_run_modify_without_nomove_flag,
        )

        self.assertEqual(observation.result.outcome, RETAG_RETAGGED)
        with self.assertRaises(AssertionError) as caught:
            check_real_modify_retag_moved_every_identity(
                observation,
                expected_artist=INSTALLED_ARTIST,
                expected_album_dir_name=f"{INSTALLED_ALBUM_DIR_NAME} [{MERGED}]",
            )
        self.assertIn("RELOCATED", str(caught.exception))

    def test_dropping_nomove_also_loses_the_sidecar_to_the_prune_sweep(
        self,
    ) -> None:
        """The SAME mutant, isolating the SIDECAR clause independently
        (per-clause proof, `docs/generated-testing.md` § "Per-clause
        proof"): told the file's NEW (post-move) directory is the correct
        one — so the relocation clause does not itself trip — the sidecar
        clause still must catch that ``cratedigger.json`` did not follow
        the move; only the tracked item file did."""
        observation = _observe_identity_relevant_path_retag(
            run_modify=_run_modify_without_nomove_flag,
        )

        self.assertEqual(observation.result.outcome, RETAG_RETAGGED)
        with self.assertRaisesRegex(
            AssertionError, "sidecar is gone from the album directory",
        ):
            check_real_modify_retag_moved_every_identity(
                observation,
                expected_artist=INSTALLED_ARTIST,
                expected_album_dir_name=f"{INSTALLED_ALBUM_DIR_NAME} [{SURVIVOR}]",
            )


# ---------------------------------------------------------------------------
# #1093 item 3 — the real deployment plugin list, not plugins: ""
# ---------------------------------------------------------------------------

class TestRealModifyRetagWithDeploymentPlugins(unittest.TestCase):
    """#1093 item 3 — every real-Beets fixture above runs with ``plugins:
    ""``, while production loads the full deployment list
    (``examples/cratedigger.nix``). #1087's review closed this gap BY HAND
    — a reviewer read every deployed plugin's ``register_listener`` calls
    and found none fire on ``modify -a -M -W -y`` — but the TESTS never
    encoded that, so a future plugin gaining a ``write`` or
    ``database_change`` listener would not be caught here.

    Composes the REAL ``retag_merged_album``, a REAL ``beet modify``
    subprocess, and a library configured with the REAL deployment plugin
    list — derived from ``examples/cratedigger.nix`` via
    ``tests.beets_world.extract_consumer_beets_world_config``, never
    hand-typed, so a plugin added to or removed from the deployment config
    is picked up automatically — proving the primitive's observable
    behavior is unchanged: identity moves, no file relocates, the sidecar
    survives, no file's mtime changes. Not merely that the plugins load.
    """

    def test_the_full_deployment_plugin_list_loads_and_behaves_identically(
        self,
    ) -> None:
        deployment_plugins = extract_consumer_beets_world_config(
            REPO_ROOT,
        ).deployment_plugins

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, album_id = _seed_real_modify_world(
                base, item_count=1, plugins=deployment_plugins,
            )
            track_files = _track_files(root)
            item_mtimes_before_ns = tuple(
                path.stat().st_mtime_ns for path in track_files
            )
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ), BeetsDB(str(library_db), library_root=str(root)) as beets:
                result = retag_merged_album(
                    beets, old_identity=OLD, new_identity=NEW,
                )
            item_mtimes_after_ns = tuple(
                path.stat().st_mtime_ns for path in track_files
            )

            lib = beets_library.Library(str(library_db), str(root))
            album = lib.get_album(album_id)
            if album is None:
                raise AssertionError("the seeded album vanished from the library")
            items = list(album.items())
            observation = RealModifyObservation(
                item_count=1,
                variant="deployment_plugins",
                result=result,
                album_mb_albumid=str(album.mb_albumid),
                item_mb_albumids=tuple(str(item.mb_albumid) for item in items),
                item_paths=tuple(os.fsdecode(item.path) for item in items),
                installed_dir_entries=tuple(sorted(
                    entry.name for entry in _installed_dir(root).iterdir()
                )) if _installed_dir(root).exists() else (),
                item_mtimes_before_ns=item_mtimes_before_ns,
                item_mtimes_after_ns=item_mtimes_after_ns,
            )
            lib._close()

        check_real_modify_retag_moved_every_identity(observation)

    def test_the_deployment_plugin_list_actually_loads(self) -> None:
        """#1093 item 3 review F3 — the sibling test above proves the retag
        still behaves correctly under the full plugin list, but nothing
        asserted the plugins actually LOADED. Two live survivors: reverting
        the fixture to ``plugins=()`` still passes it (no mutant at this
        diff site), and a plugin silently failing to load (Beets logs
        ``error loading plugin`` and continues; the returncode this module
        deliberately never trusts stays 0) would too — a future regression
        could quietly degrade the deployment-fidelity fixture back to
        ``plugins: ""`` fidelity, the exact failure #1093 review F3 named.

        ``-v`` is added ONLY here, never on the production
        ``run_beets_modify_retag`` argv: it raises Beets' own logging to
        DEBUG (``beets/ui/__init__.py``), where ``beets/plugins.py``
        already logs ``Loading plugins: {sorted names}`` — a pure
        observability addition with no effect on move/write/query
        semantics, live-verified against this exact fixture."""
        deployment_plugins = extract_consumer_beets_world_config(
            REPO_ROOT,
        ).deployment_plugins

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            _root, _library_db, _album_id = _seed_real_modify_world(
                base, item_count=1, plugins=deployment_plugins,
            )
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ):
                from lib.util import beets_subprocess_env

                env = beets_subprocess_env()
            proc = sp.run(
                [
                    env["CRATEDIGGER_BEETS_PYTHON"], "-m", "beets", "-v", "modify",
                    "-a", "-M", "-W", "-y",
                    f"mb_albumid:={MERGED}", f"mb_albumid={SURVIVOR}",
                ],
                capture_output=True, env=env, timeout=RETAG_TIMEOUT_SECONDS,
                check=False,
            )
        stderr = proc.stderr.decode("utf-8", errors="replace")

        self.assertEqual(proc.returncode, 0, stderr)
        self.assertNotIn(
            "error loading plugin", stderr,
            "a deployed plugin failed to load — the fixture silently "
            "degraded to a narrower plugin set than the deployment list",
        )
        loading_line = next(
            (line for line in stderr.splitlines() if "Loading plugins:" in line),
            None,
        )
        self.assertIsNotNone(
            loading_line,
            "beet -v never printed a \"Loading plugins:\" line at all — "
            "cannot confirm what actually loaded",
        )
        assert loading_line is not None  # narrow for pyright
        loaded = {
            name.strip()
            for name in loading_line.split("Loading plugins:", 1)[1].split(",")
        }
        self.assertEqual(
            loaded, set(deployment_plugins),
            "the loaded plugin set does not match the deployment list — "
            "the fixture silently degraded",
        )


# ---------------------------------------------------------------------------
# #1093 review residual — the compound query closes the TIME-OF-CHECK/
# TIME-OF-USE gap a value-only re-select leaves open
# ---------------------------------------------------------------------------

class TestCompoundQueryClosesTheTimeOfCheckTimeOfUseRace(unittest.TestCase):
    """The guard resolves the row on one connection; ``beet modify``
    re-selects on a SEPARATE connection at a LATER time. Both halves of the
    compound query (``id:=<album_id>`` AND ``mb_albumid:=<old-id>``) are
    load-bearing against that gap, proven with a REAL ``beet modify``
    subprocess round trip in both directions: the id alone would be a
    blind write (retagging whoever now holds that primary key regardless
    of value); the value alone would retag whoever now holds that value
    regardless of which specific row the guard actually authorized.
    """

    def test_a_second_album_sharing_the_value_is_never_touched_by_the_guards_query(
        self,
    ) -> None:
        """The id clause pins the EXACT row the guard resolved. Seeds a
        genuine two-album-SAME-VALUE world (the shape a raw third-party
        write — or an organic race window before beets' own ambiguity
        check runs — could produce) and builds the query via the REAL
        :func:`retag_album_query`, pinned to the FIRST album's id — as if
        the guard had resolved uniquely at a moment before the second
        album shared this value. Only the id-pinned row may ever move,
        even though BOTH match by value alone; a mutant that disables or
        drops the id clause would retag both."""
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            _root, library_db, target_id, second_id = _seed_two_album_world(
                base, phantom_identity=MERGED, phantom_as_blob=False,
            )
            query_tokens = retag_album_query(OLD, album_id=target_id)
            assignment = retag_assignment(NEW)

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ):
                run = run_beets_modify_retag(query_tokens, assignment)

            self.assertEqual(run.returncode, 0, run.stdout)

            conn = sqlite3.connect(str(library_db))
            target_row = conn.execute(
                "SELECT mb_albumid FROM albums WHERE id = ?", (target_id,),
            ).fetchone()
            second_row = conn.execute(
                "SELECT mb_albumid FROM albums WHERE id = ?", (second_id,),
            ).fetchone()
            conn.close()

            self.assertEqual(target_row, (SURVIVOR,))
            self.assertEqual(
                second_row, (MERGED,),
                "a second album sharing the SAME value must never move — "
                "only the id-pinned row may, regardless of how many other "
                "rows match by value alone",
            )

    def test_a_correct_id_with_a_changed_value_is_refused_untouched(self) -> None:
        """The value clause is not redundant with the id clause: an album
        whose ``mb_albumid`` has ALREADY changed since the guard's read
        (simulating a concurrent retag of this exact row) must not be
        blindly overwritten by primary key alone."""
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, target_id = _seed_real_modify_world(
                base, item_count=1,
            )
            changed_value = "99999999-0000-4000-8000-000000000000"
            lib = beets_library.Library(str(library_db), str(root))
            album = lib.get_album(target_id)
            if album is None:
                raise AssertionError("the seeded album vanished from the library")
            album.mb_albumid = changed_value
            album.store()
            lib._close()

            # The stale query still names the OLD (pre-race) value, as a
            # guard resolution taken before the race would.
            query_tokens = retag_album_query(OLD, album_id=target_id)
            assignment = retag_assignment(NEW)

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ):
                run = run_beets_modify_retag(query_tokens, assignment)

            self.assertNotEqual(run.returncode, 0, run.stdout)

            lib = beets_library.Library(str(library_db), str(root))
            album = lib.get_album(target_id)
            if album is None:
                raise AssertionError("the seeded album vanished from the library")
            self.assertEqual(
                album.mb_albumid, changed_value,
                "a changed value must never be overwritten by primary key alone",
            )
            lib._close()

    def test_the_correct_id_and_value_together_retag(self) -> None:
        """Must-still-work: when nothing raced, both clauses matching the
        SAME row retags it, exactly as the composed production path does."""
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            root, library_db, target_id = _seed_real_modify_world(
                base, item_count=1,
            )
            query_tokens = retag_album_query(OLD, album_id=target_id)
            assignment = retag_assignment(NEW)

            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(base / "config.ini")},
                clear=False,
            ):
                run = run_beets_modify_retag(query_tokens, assignment)

            self.assertEqual(run.returncode, 0, run.stdout)

            lib = beets_library.Library(str(library_db), str(root))
            album = lib.get_album(target_id)
            if album is None:
                raise AssertionError("the seeded album vanished from the library")
            self.assertEqual(album.mb_albumid, SURVIVOR)
            lib._close()


if __name__ == "__main__":
    unittest.main()
