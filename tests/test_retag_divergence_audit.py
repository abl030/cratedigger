"""Deterministic pins for the retag ``-W`` divergence census (#1093 item 1).

``lib/beets_retag.py``'s ``-W`` is deliberate and stays (see its module
docstring): a successful retag moves the Beets DB's ``mb_albumid`` without
touching any installed file's tag. This module's contract is the read-only
census of that residual — never a reconciler. The generated siblings in
``tests/test_retag_divergence_audit_generated.py`` patrol the world space
these pins anchor.

``TestExactWResidualRegressionPin`` mirrors the real merge probed on
2026-08-06 (request 316, MERGED/SURVIVOR from
``tests/test_beets_retag.py``): an album whose DB ``mb_albumid`` moved to
the SURVIVOR while every installed file's tag still names the merged-away
id must be classified ``diverges`` and listed.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import ClassVar

from mediafile import MediaFile

from lib.beets_db import BeetsAlbumIdentityRow, BeetsDB
from lib.retag_divergence_audit import (
    RetagDivergenceItem,
    RetagDivergenceItemClass,
    TagReadOk,
    TagReadUnreadable,
    album_class_from_items,
    classify_retag_divergence_item,
    scan_retag_divergence,
    scan_retag_divergence_from_borrowed_factory,
    scan_retag_divergence_from_factory,
)
from tests.fakes import FakeBeetsDB
from tests.test_beets_db import _create_test_db, _insert_album
from tests.test_beets_retag import MERGED, SURVIVOR, _make_real_mp3

_SQLITE_AVAILABILITY_FAILURES: tuple[
    tuple[int, type[sqlite3.DatabaseError]], ...
] = (
    (sqlite3.SQLITE_AUTH, sqlite3.DatabaseError),
    (sqlite3.SQLITE_BUSY, sqlite3.OperationalError),
    (sqlite3.SQLITE_CANTOPEN, sqlite3.OperationalError),
    (sqlite3.SQLITE_IOERR, sqlite3.OperationalError),
    (sqlite3.SQLITE_LOCKED, sqlite3.OperationalError),
    (sqlite3.SQLITE_PERM, sqlite3.OperationalError),
)


def _sqlite_availability_failure(
    code: int, error_type: type[sqlite3.DatabaseError],
) -> sqlite3.DatabaseError:
    failure = error_type(f"sqlite authority failure {code}")
    failure.sqlite_errorcode = code
    return failure


def _read_tag_from_map(
    mapping: dict[str, str | Exception],
) -> Callable[[str], str]:
    """Injected ``read_tag`` — a path maps to either a tag value or a
    raised exception, standing in for :func:`read_mb_albumid_tag`."""

    def read(path: str) -> str:
        value = mapping[path]
        if isinstance(value, Exception):
            raise value
        return value

    return read


class TestClassifyRetagDivergenceItem(unittest.TestCase):
    """Pure per-item classifier (`lib/retag_divergence_audit.py`) — every branch."""

    CASES: ClassVar = [
        ("db and file agree", SURVIVOR, TagReadOk(mb_albumid=SURVIVOR), "agrees"),
        ("db and file both absent", "", TagReadOk(mb_albumid=""), "agrees"),
        (
            "the -W residual: db moved, file still names the merged-away id",
            SURVIVOR, TagReadOk(mb_albumid=MERGED), "diverges",
        ),
        (
            "db present, file tag blank",
            SURVIVOR, TagReadOk(mb_albumid=""), "diverges",
        ),
        (
            (
                "db absent, file still carries an id "
                "(Discogs-neutralization shape, #570)"
            ),
            "", TagReadOk(mb_albumid=MERGED), "file_tag_present_db_absent",
        ),
        (
            "unreadable file never counts as agreeing",
            SURVIVOR, TagReadUnreadable(detail="boom"), "unreadable",
        ),
        (
            "unreadable file with an absent db identity still fails closed",
            "", TagReadUnreadable(detail="boom"), "unreadable",
        ),
        (
            "case differences normalize before comparing",
            SURVIVOR.upper(), TagReadOk(mb_albumid=SURVIVOR), "agrees",
        ),
    ]

    def test_every_branch(self) -> None:
        for desc, db_mb_albumid, file_tag, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(
                    classify_retag_divergence_item(
                        db_mb_albumid=db_mb_albumid, file_tag=file_tag,
                    ),
                    expected,
                )


def _item(
    item_class: RetagDivergenceItemClass, path: str = "/x.mp3",
) -> RetagDivergenceItem:
    if item_class == "unreadable":
        return RetagDivergenceItem(
            path=path, item_class="unreadable",
            file_mb_albumid=None, detail="boom",
        )
    return RetagDivergenceItem(
        path=path, item_class=item_class, file_mb_albumid="x", detail=None,
    )


class TestAlbumClassFromItems(unittest.TestCase):
    """Precedence: unreadable > diverges > file_tag_present_db_absent > agrees."""

    CASES: ClassVar = [
        ("no items", (), "empty"),
        ("all agree", (_item("agrees"),), "agrees"),
        (
            "one diverges among agreeing items",
            (_item("agrees"), _item("diverges")), "diverges",
        ),
        (
            "unreadable outranks diverges",
            (_item("diverges"), _item("unreadable")), "unreadable",
        ),
        (
            "diverges outranks file_tag_present_db_absent",
            (_item("file_tag_present_db_absent"), _item("diverges")),
            "diverges",
        ),
        (
            "file_tag_present_db_absent alone",
            (_item("agrees"), _item("file_tag_present_db_absent")),
            "file_tag_present_db_absent",
        ),
        (
            "unreadable outranks everything",
            (
                _item("diverges"), _item("file_tag_present_db_absent"),
                _item("unreadable"), _item("agrees"),
            ),
            "unreadable",
        ),
    ]

    def test_precedence(self) -> None:
        for desc, items, expected in self.CASES:
            with self.subTest(desc=desc):
                self.assertEqual(album_class_from_items(items), expected)


class TestScanRetagDivergence(unittest.TestCase):
    """Pure orchestration — real composition, injected leaf reader."""

    def test_clean_report_lists_nothing(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=1, mb_albumid=SURVIVOR, item_paths=("/a/01.mp3",),
            ),
        ])

        report = scan_retag_divergence(
            beets, read_tag=_read_tag_from_map({"/a/01.mp3": SURVIVOR}),
        )

        self.assertEqual(report.status, "clean")
        self.assertTrue(report.complete)
        self.assertEqual(report.albums, ())
        self.assertEqual(report.counts.albums_scanned, 1)
        self.assertEqual(report.counts.items_read, 1)
        self.assertEqual(report.counts.items_unreadable, 0)

    def test_diverging_album_is_listed_and_counted(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=5, mb_albumid=SURVIVOR,
                item_paths=("/a/01.mp3", "/a/02.mp3"),
            ),
        ])

        report = scan_retag_divergence(
            beets, read_tag=_read_tag_from_map({
                "/a/01.mp3": MERGED, "/a/02.mp3": SURVIVOR,
            }),
        )

        self.assertEqual(report.status, "divergence_found")
        self.assertEqual(len(report.albums), 1)
        album = report.albums[0]
        self.assertEqual(album.album_id, 5)
        self.assertEqual(album.album_class, "diverges")
        self.assertEqual(album.item_count, 2)
        self.assertEqual(report.counts.albums_diverging, 1)
        self.assertEqual(report.counts.albums_scanned, 1)
        self.assertEqual(report.counts.items_read, 2)

    def test_discogs_neutralization_shape_gets_its_own_bucket(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=2, mb_albumid="", item_paths=("/a/01.mp3",),
            ),
        ])

        report = scan_retag_divergence(
            beets, read_tag=_read_tag_from_map({"/a/01.mp3": MERGED}),
        )

        self.assertEqual(len(report.albums), 1)
        self.assertEqual(
            report.albums[0].album_class, "file_tag_present_db_absent",
        )
        self.assertEqual(report.counts.albums_file_tag_present_db_absent, 1)
        self.assertEqual(report.counts.albums_diverging, 0)

    def test_unreadable_file_is_never_agrees_even_when_db_is_absent(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=3, mb_albumid="", item_paths=("/a/01.mp3",),
            ),
        ])

        report = scan_retag_divergence(
            beets, read_tag=_read_tag_from_map({"/a/01.mp3": OSError("boom")}),
        )

        self.assertEqual(len(report.albums), 1)
        self.assertEqual(report.albums[0].album_class, "unreadable")
        self.assertEqual(report.counts.items_unreadable, 1)
        self.assertEqual(report.counts.albums_unreadable, 1)

    def test_zero_item_album_is_empty_not_agrees(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=4, mb_albumid=SURVIVOR, item_paths=(),
            ),
        ])

        report = scan_retag_divergence(beets, read_tag=_read_tag_from_map({}))

        self.assertEqual(len(report.albums), 1)
        self.assertEqual(report.albums[0].album_class, "empty")
        self.assertEqual(report.counts.albums_empty, 1)

    def test_albums_are_reported_sorted_by_id(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=9, mb_albumid=SURVIVOR, item_paths=("/b/01.mp3",),
            ),
            BeetsAlbumIdentityRow(
                album_id=2, mb_albumid=SURVIVOR, item_paths=("/a/01.mp3",),
            ),
        ])

        report = scan_retag_divergence(
            beets,
            read_tag=_read_tag_from_map({
                "/a/01.mp3": MERGED, "/b/01.mp3": MERGED,
            }),
        )

        self.assertEqual([album.album_id for album in report.albums], [2, 9])


class TestExactWResidualRegressionPin(unittest.TestCase):
    """The exact scenario issue #1093 item 1 exists to surface."""

    def test_survivor_db_with_merged_away_file_tags_is_flagged(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=42, mb_albumid=SURVIVOR,
                item_paths=(
                    "/library/Album/01.flac", "/library/Album/02.flac",
                ),
            ),
        ])

        report = scan_retag_divergence(
            beets,
            read_tag=_read_tag_from_map({
                "/library/Album/01.flac": MERGED,
                "/library/Album/02.flac": MERGED,
            }),
        )

        self.assertEqual(report.status, "divergence_found")
        self.assertEqual(len(report.albums), 1)
        album = report.albums[0]
        self.assertEqual(album.album_id, 42)
        self.assertEqual(album.db_mb_albumid, SURVIVOR)
        self.assertEqual(album.album_class, "diverges")
        self.assertEqual(
            {item.item_class for item in album.items}, {"diverges"},
        )
        self.assertEqual(
            [item.file_mb_albumid for item in album.items], [MERGED, MERGED],
        )


class TestScanRetagDivergenceAvailability(unittest.TestCase):
    """Owning/borrowed factory availability mediation, mirroring
    ``tests/test_world_audit_service.py``'s pattern for the one query
    this module has."""

    def test_expected_open_failures_are_an_unavailable_report(self) -> None:
        sqlite_failures = [
            _sqlite_availability_failure(code, error_type)
            for code, error_type in _SQLITE_AVAILABILITY_FAILURES
        ]
        for failure in (
            FileNotFoundError("missing"),
            PermissionError("denied"),
            *sqlite_failures,
        ):
            with self.subTest(failure=type(failure).__name__):
                def unavailable_factory(error: Exception = failure) -> FakeBeetsDB:
                    raise error

                report = scan_retag_divergence_from_factory(unavailable_factory)

                self.assertEqual(report.status, "beets_unavailable")
                self.assertFalse(report.complete)
                self.assertIsNotNone(report.unavailable_detail)

    def test_expected_query_failures_are_unavailable_and_still_close(self) -> None:
        class QueryFailureBeets(FakeBeetsDB):
            def __init__(self, failure: sqlite3.DatabaseError) -> None:
                super().__init__()
                self.failure = failure

            def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
                raise self.failure

        for code, error_type in _SQLITE_AVAILABILITY_FAILURES:
            with self.subTest(code=code):
                failure = _sqlite_availability_failure(code, error_type)
                beets = QueryFailureBeets(failure)

                report = scan_retag_divergence_from_factory(
                    lambda handle=beets: handle,
                )

                self.assertEqual(report.status, "beets_unavailable")
                self.assertFalse(report.complete)
                self.assertEqual(beets.close_calls, 1)

    def test_unexpected_open_failure_propagates(self) -> None:
        def broken_factory() -> FakeBeetsDB:
            raise RuntimeError("programmer defect")

        with self.assertRaisesRegex(RuntimeError, "programmer defect"):
            scan_retag_divergence_from_factory(broken_factory)

    def test_unexpected_query_and_close_failures_propagate(self) -> None:
        class BrokenQueryBeets(FakeBeetsDB):
            def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
                raise RuntimeError("query programmer defect")

        query_beets = BrokenQueryBeets()
        with self.assertRaisesRegex(RuntimeError, "query programmer defect"):
            scan_retag_divergence_from_factory(lambda: query_beets)
        self.assertEqual(query_beets.close_calls, 1)

        class BrokenCloseBeets(FakeBeetsDB):
            def close(self) -> None:
                super().close()
                raise RuntimeError("close programmer defect")

        close_beets = BrokenCloseBeets()
        with self.assertRaisesRegex(RuntimeError, "close programmer defect"):
            scan_retag_divergence_from_factory(lambda: close_beets)
        self.assertEqual(close_beets.close_calls, 1)

    def test_factory_owned_handle_is_closed(self) -> None:
        beets = FakeBeetsDB()

        report = scan_retag_divergence_from_factory(lambda: beets)

        self.assertEqual(report.status, "clean")
        self.assertEqual(beets.close_calls, 1)

    def test_borrowed_handle_is_never_closed(self) -> None:
        beets = FakeBeetsDB()

        report = scan_retag_divergence_from_borrowed_factory(lambda: beets)

        self.assertEqual(report.status, "clean")
        self.assertEqual(beets.close_calls, 0)

    def test_borrowed_handle_not_closed_when_query_is_unavailable(self) -> None:
        class UnavailableBeets(FakeBeetsDB):
            def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
                failure = sqlite3.OperationalError("database is locked")
                failure.sqlite_errorcode = sqlite3.SQLITE_LOCKED
                raise failure

        beets = UnavailableBeets()

        report = scan_retag_divergence_from_borrowed_factory(lambda: beets)

        self.assertFalse(report.complete)
        self.assertEqual(beets.close_calls, 0)


class TestRealRetagDivergenceScan(unittest.TestCase):
    """Real files, a real Beets SQLite DB, and the real ``mediafile``
    reader — reuses ``_make_real_mp3`` from ``tests/test_beets_retag.py``
    (#1093 review guidance) rather than duplicating the ffmpeg invocation.
    """

    def test_real_files_classify_correctly_against_the_real_reader(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            library_root = os.path.join(tmpdir, "library")
            db_path = os.path.join(tmpdir, "library.db")
            _create_test_db(db_path)

            agree_dir = os.path.join(library_root, "Agree")
            diverge_dir = os.path.join(library_root, "Diverge")
            absent_dir = os.path.join(library_root, "Absent")
            corrupt_dir = os.path.join(library_root, "Corrupt")
            for directory in (agree_dir, diverge_dir, absent_dir, corrupt_dir):
                os.makedirs(directory)

            agree_path = Path(agree_dir) / "01.mp3"
            diverge_path = Path(diverge_dir) / "01.mp3"
            absent_path = Path(absent_dir) / "01.mp3"
            corrupt_path = Path(corrupt_dir) / "01.mp3"

            _make_real_mp3(agree_path)
            _make_real_mp3(diverge_path)
            _make_real_mp3(absent_path)
            corrupt_path.write_bytes(b"not an mp3 at all")

            agree_media = MediaFile(agree_path)
            agree_media.mb_albumid = SURVIVOR
            agree_media.save()

            diverge_media = MediaFile(diverge_path)
            diverge_media.mb_albumid = MERGED
            diverge_media.save()

            # The Discogs-neutralization shape: DB carries no mb_albumid
            # for this album, but the file's tag still names one.
            absent_media = MediaFile(absent_path)
            absent_media.mb_albumid = MERGED
            absent_media.save()

            _insert_album(
                db_path, 1, SURVIVOR,
                [(32000, os.path.relpath(agree_path, library_root))],
            )
            _insert_album(
                db_path, 2, SURVIVOR,
                [(32000, os.path.relpath(diverge_path, library_root))],
            )
            _insert_album(
                db_path, 3, "",
                [(32000, os.path.relpath(absent_path, library_root))],
            )
            _insert_album(
                db_path, 4, SURVIVOR,
                [(32000, os.path.relpath(corrupt_path, library_root))],
            )

            with BeetsDB(db_path, library_root=library_root) as beets:
                report = scan_retag_divergence(beets)

        self.assertEqual(report.counts.albums_scanned, 4)
        self.assertEqual(report.counts.items_read, 4)
        self.assertEqual(report.counts.items_unreadable, 1)
        by_id = {album.album_id: album for album in report.albums}
        self.assertNotIn(1, by_id, "an agreeing album must never be listed")
        self.assertEqual(by_id[2].album_class, "diverges")
        self.assertEqual(by_id[2].items[0].file_mb_albumid, MERGED)
        self.assertEqual(by_id[3].album_class, "file_tag_present_db_absent")
        self.assertEqual(by_id[4].album_class, "unreadable")
        self.assertIsNotNone(by_id[4].items[0].detail)


if __name__ == "__main__":
    unittest.main()
