#!/usr/bin/env python3
"""Pinned-Beets and filesystem contracts for exact library deletion."""

from __future__ import annotations

import os
import sqlite3
import subprocess as sp
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import msgspec
from beets import library

from lib.beets_db import BeetsDB
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailed,
    BeetsDeleteRequest,
    _OwnedPath,
    _confined_path,
    _delete_manifest,
    _path_exists,
    _remove_album_metadata_atomically,
    run_beets_delete,
)


RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


class TestPinnedBeetsDelete(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "library"
        self.root.mkdir()
        self.db_path = Path(self.tmp.name) / "beets.db"
        self.config_dir = Path(self.tmp.name) / "config"
        self.config_dir.mkdir()
        (self.config_dir / "config.yaml").write_text(
            f"directory: {self.root}\n"
            f"library: {self.db_path}\n"
            "plugins: []\n"
            "clutter: ['*.jpg', 'cratedigger.json']\n",
            encoding="utf-8",
        )
        self.runtime_config = Path(self.tmp.name) / "config.ini"
        self.runtime_config.write_text(
            "[Beets]\n"
            f"directory = {self.root}\n"
            f"config_dir = {self.config_dir}\n"
            f"python = {sys.executable}\n",
            encoding="utf-8",
        )

    def _seed(self, *, relative: bool = True) -> tuple[int, Path]:
        album_dir = self.root / "Artist" / "Album"
        album_dir.mkdir(parents=True)
        track = album_dir / "01 Track.flac"
        track.write_bytes(b"audio")
        path: str | bytes = (
            str(track.relative_to(self.root)) if relative else str(track)
        )
        lib = library.Library(str(self.db_path), str(self.root))
        item = library.Item(
            path=path,
            album="Album",
            albumartist="Artist",
            artist="Artist",
            title="Track",
            mb_albumid=RELEASE,
        )
        album = lib.add_album([item])
        album.artpath = os.fsencode(album_dir / "cover.jpg")
        album["delete_album_flex"] = "album-flex"
        album.store()
        item["delete_item_flex"] = "item-flex"
        item.store()
        album_id = int(album.id)
        lib._close()
        return album_id, album_dir

    def _run(self, album_id: int):
        with patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(self.runtime_config),
        }):
            return run_beets_delete(BeetsDeleteRequest(
                album_id=album_id,
                expected_release_id=RELEASE,
                library_db_path=str(self.db_path),
                library_root=str(self.root),
            ))

    def test_relative_tracks_art_sidecar_and_clutter_use_real_pinned_beets(self) -> None:
        album_id, album_dir = self._seed(relative=True)
        (album_dir / "cover.jpg").write_bytes(b"art")
        (album_dir / "cratedigger.json").write_bytes(b"sidecar")
        (album_dir / "scan.jpg").write_bytes(b"clutter")
        sentinel = album_dir / "booklet.pdf"
        sentinel.write_bytes(b"preserve me")

        result = self._run(album_id)

        self.assertIsInstance(result, BeetsDeleteCompleted)
        assert isinstance(result, BeetsDeleteCompleted)
        self.assertEqual(result.deleted_tracks, 1)
        self.assertEqual(result.deleted_artifacts, 4)
        self.assertEqual(result.preserved_paths, (str(sentinel),))
        self.assertEqual(sentinel.read_bytes(), b"preserve me")
        with BeetsDB(str(self.db_path), library_root=str(self.root)) as beets:
            self.assertIsNone(beets.get_album_detail(album_id))

    def test_symlink_escape_is_rejected_before_mutation(self) -> None:
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        escaped = outside / "track.flac"
        escaped.write_bytes(b"rare")
        link = self.root / "escape"
        link.symlink_to(outside, target_is_directory=True)

        self.assertIsNone(_confined_path(link / "track.flac", self.root.resolve()))
        self.assertEqual(escaped.read_bytes(), b"rare")

    def test_active_database_or_root_mismatch_is_zero_mutation(self) -> None:
        album_id, album_dir = self._seed()
        track = album_dir / "01 Track.flac"
        alternate_root = Path(self.tmp.name) / "other-library"
        alternate_root.mkdir()
        alternate_db = Path(self.tmp.name) / "other.db"
        alternate_db.touch()
        for db_path, root in (
            (alternate_db, self.root),
            (self.db_path, alternate_root),
        ):
            with self.subTest(db_path=db_path, root=root), patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": str(self.runtime_config)},
            ):
                result = run_beets_delete(BeetsDeleteRequest(
                    album_id=album_id,
                    expected_release_id=RELEASE,
                    library_db_path=str(db_path),
                    library_root=str(root),
                ))

            self.assertIsInstance(result, BeetsDeleteFailed)
            assert isinstance(result, BeetsDeleteFailed)
            self.assertEqual(result.reason, "configuration_mismatch")
            self.assertTrue(track.exists())
            with BeetsDB(
                str(self.db_path), library_root=str(self.root),
            ) as beets:
                self.assertIsNotNone(beets.get_album_detail(album_id))

    def test_subprocess_argv_and_malformed_json_protocol_fail_closed(self) -> None:
        album_id, _album_dir = self._seed()
        calls: list[tuple[list[str], dict[str, object]]] = []

        def runner(argv, **kwargs):
            calls.append((argv, kwargs))
            return sp.CompletedProcess(argv, 0, stdout=b"{", stderr=b"")

        with patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(self.runtime_config),
        }):
            result = run_beets_delete(
                BeetsDeleteRequest(
                    album_id=album_id,
                    expected_release_id=RELEASE,
                    library_db_path=str(self.db_path),
                    library_root=str(self.root),
                ),
                runner=runner,
            )

        self.assertIsInstance(result, BeetsDeleteFailed)
        assert isinstance(result, BeetsDeleteFailed)
        self.assertEqual(result.reason, "protocol_error")
        self.assertEqual(len(calls), 1)
        argv, kwargs = calls[0]
        self.assertEqual(argv[0], sys.executable)
        self.assertTrue(argv[1].endswith("/harness/delete_album.py"))
        self.assertEqual(
            kwargs["input"],
            msgspec.json.encode(BeetsDeleteRequest(
                album_id=album_id,
                expected_release_id=RELEASE,
                library_db_path=str(self.db_path),
                library_root=str(self.root),
            )),
        )

    def test_metadata_kill_equivalent_rolls_back_album_items_and_flex_rows(
        self,
    ) -> None:
        album_id, _album_dir = self._seed()
        lib = library.Library(str(self.db_path), str(self.root))
        album = lib.get_album(album_id)
        assert album is not None
        with (
            patch.object(library.Item, "remove", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            _remove_album_metadata_atomically(lib, album)
        lib._close()

        def counts() -> tuple[int, int, int, int]:
            with closing(sqlite3.connect(self.db_path)) as conn:
                return (
                    int(conn.execute(
                        "SELECT COUNT(*) FROM albums WHERE id = ?", (album_id,),
                    ).fetchone()[0]),
                    int(conn.execute(
                        "SELECT COUNT(*) FROM items WHERE album_id = ?", (album_id,),
                    ).fetchone()[0]),
                    int(conn.execute(
                        "SELECT COUNT(*) FROM album_attributes "
                        "WHERE entity_id = ? AND key = 'delete_album_flex'",
                        (album_id,),
                    ).fetchone()[0]),
                    int(conn.execute(
                        "SELECT COUNT(*) FROM item_attributes "
                        "WHERE key = 'delete_item_flex'",
                    ).fetchone()[0]),
                )

        self.assertEqual(counts(), (1, 1, 1, 1))

        lib = library.Library(str(self.db_path), str(self.root))
        album = lib.get_album(album_id)
        assert album is not None
        _remove_album_metadata_atomically(lib, album)
        lib._close()
        self.assertEqual(counts(), (0, 0, 0, 0))


class TestDeleteManifestOrdering(unittest.TestCase):
    def test_strict_presence_probe_only_treats_not_found_as_absent(self) -> None:
        def missing(_path: str) -> os.stat_result:
            raise FileNotFoundError

        def io_fault(_path: str) -> os.stat_result:
            raise OSError("planted stat I/O fault")

        self.assertFalse(_path_exists("/library/missing.flac", lstat=missing))
        with self.assertRaisesRegex(OSError, "planted stat I/O fault"):
            _path_exists("/library/unreadable.flac", lstat=io_fault)

    def test_presence_probe_errors_fail_closed_at_every_phase(self) -> None:
        scenarios = (
            ("pre", 1, False, "filesystem_error", True),
            ("post", 2, False, "postcondition_failed", False),
            ("progress", 2, True, "filesystem_error", True),
            ("final", 3, False, "postcondition_failed", False),
        )
        for phase, fault_at, removal_fault, reason, track_survives in scenarios:
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as raw:
                track = Path(raw) / "01.flac"
                track.write_bytes(b"audio")
                metadata_present = True
                probe_calls = 0

                def probe(path: str) -> bool:
                    nonlocal probe_calls
                    probe_calls += 1
                    if probe_calls == fault_at:
                        raise OSError(f"planted {phase} presence fault")
                    try:
                        os.lstat(path)
                    except FileNotFoundError:
                        return False
                    return True

                def remove(path: str) -> None:
                    if removal_fault:
                        raise OSError("planted removal fault")
                    os.remove(path)

                def remove_metadata() -> None:
                    nonlocal metadata_present
                    metadata_present = False

                outcome = _delete_manifest(
                    album_id=7,
                    album_name="Album",
                    artist_name="Artist",
                    owned_paths=(_OwnedPath(str(track), "track"),),
                    album_dirs=(raw,),
                    metadata_remove=remove_metadata,
                    album_present=lambda: metadata_present,
                    remove_path=remove,
                    prune_dir=lambda _path: None,
                    path_exists=probe,
                )

                self.assertIsInstance(outcome, BeetsDeleteFailed)
                assert isinstance(outcome, BeetsDeleteFailed)
                self.assertEqual(outcome.reason, reason)
                self.assertIn("presence probe", outcome.detail)
                self.assertTrue(metadata_present)
                self.assertEqual(track.exists(), track_survives)
                self.assertEqual(outcome.remaining_owned_paths, (str(track),))

    def test_unknown_enumeration_error_prevents_any_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            track = Path(raw) / "01.flac"
            track.write_bytes(b"audio")
            metadata_removed = False

            def remove_metadata() -> None:
                nonlocal metadata_removed
                metadata_removed = True

            def fail_list(_directory: Path) -> tuple[Path, ...]:
                raise OSError("planted enumeration fault")

            outcome = _delete_manifest(
                album_id=7,
                album_name="Album",
                artist_name="Artist",
                owned_paths=(_OwnedPath(str(track), "track"),),
                album_dirs=(raw,),
                metadata_remove=remove_metadata,
                album_present=lambda: not metadata_removed,
                remove_path=os.remove,
                prune_dir=lambda _path: None,
                list_dir=fail_list,
            )

            self.assertIsInstance(outcome, BeetsDeleteFailed)
            assert isinstance(outcome, BeetsDeleteFailed)
            self.assertEqual(outcome.reason, "filesystem_error")
            self.assertIn("before deletion", outcome.detail)
            self.assertFalse(metadata_removed)
            self.assertTrue(track.exists())

    def test_second_path_failure_retains_metadata_and_retry_converges(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            first = root / "01.flac"
            second = root / "02.flac"
            first.write_bytes(b"one")
            second.write_bytes(b"two")
            metadata_present = True
            calls = 0

            def remove_with_fault(path: str) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("planted second-path fault")
                os.remove(path)

            def remove_metadata() -> None:
                nonlocal metadata_present
                metadata_present = False

            outcome = _delete_manifest(
                album_id=7,
                album_name="Album",
                artist_name="Artist",
                owned_paths=(
                    _OwnedPath(str(first), "track"),
                    _OwnedPath(str(second), "track"),
                ),
                album_dirs=(str(root),),
                metadata_remove=remove_metadata,
                album_present=lambda: metadata_present,
                remove_path=remove_with_fault,
                prune_dir=lambda _path: None,
            )
            self.assertIsInstance(outcome, BeetsDeleteFailed)
            self.assertTrue(metadata_present)
            self.assertFalse(first.exists())
            self.assertTrue(second.exists())

            retry = _delete_manifest(
                album_id=7,
                album_name="Album",
                artist_name="Artist",
                owned_paths=(
                    _OwnedPath(str(first), "track"),
                    _OwnedPath(str(second), "track"),
                ),
                album_dirs=(str(root),),
                metadata_remove=remove_metadata,
                album_present=lambda: metadata_present,
                remove_path=lambda path: os.remove(path) if os.path.exists(path) else None,
                prune_dir=lambda _path: None,
            )
            self.assertIsInstance(retry, BeetsDeleteCompleted)
            self.assertFalse(metadata_present)
            self.assertFalse(second.exists())

    def test_noop_remove_cannot_claim_success(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            track = Path(raw) / "01.flac"
            track.write_bytes(b"audio")
            metadata_removed = False

            def remove_metadata() -> None:
                nonlocal metadata_removed
                metadata_removed = True

            outcome = _delete_manifest(
                album_id=7,
                album_name="Album",
                artist_name="Artist",
                owned_paths=(_OwnedPath(str(track), "track"),),
                album_dirs=(raw,),
                metadata_remove=remove_metadata,
                album_present=lambda: not metadata_removed,
                remove_path=lambda _path: None,
                prune_dir=lambda _path: None,
            )
            self.assertIsInstance(outcome, BeetsDeleteFailed)
            self.assertFalse(metadata_removed)


if __name__ == "__main__":
    unittest.main()
