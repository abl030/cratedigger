#!/usr/bin/env python3
"""Real shipped-Beets contracts at CLI and request-detail API boundaries."""

from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from lib.beets_db import CurrentBeetsResolution
from lib.release_identity import ReleaseIdentity
from scripts.pipeline_cli.show import cmd_show
from tests.beets_world import BeetsWorld, BeetsWorldRelease
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase


REPO = Path(__file__).resolve().parent.parent
MB_TARGET = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MB_SIBLING = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DISCOGS_TARGET = "12856590"
SNAPSHOT_TRACKS = [
    {
        "id": 702,
        "path": "/snapshot/current/02 Omega.flac",
        "title": "Snapshot Omega Δ",
        "track": 2,
        "disc": 1,
        "length": 222.25,
        "format": "FLAC",
        "bitrate": 922_000,
        "samplerate": 96_000,
        "bitdepth": 24,
    },
    {
        "id": 701,
        "path": "/snapshot/current/01 Alpha.flac",
        "title": "Snapshot Alpha λ",
        "track": 1,
        "disc": 1,
        "length": 111.5,
        "format": "FLAC",
        "bitrate": 811_000,
        "samplerate": 88_200,
        "bitdepth": 24,
    },
]
EXPECTED_SNAPSHOT_TRACKS = [
    {key: track[key] for key in (
        "title", "track", "disc", "length", "format", "bitrate",
        "samplerate", "bitdepth",
    )}
    for track in reversed(SNAPSHOT_TRACKS)
]


class _FailingResolverBeets(FakeBeetsDB):
    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution:
        self.resolve_current_release_calls.append(identity)
        raise OSError("synthetic Beets read failure")


def _fail_open_beets(
    *,
    path: str | None,
    library_root: str | None,
) -> FakeBeetsDB:
    del path, library_root
    raise OSError("synthetic Beets open failure")


def _snapshot_beets(release_id: str) -> FakeBeetsDB:
    beets = FakeBeetsDB(library_root="/snapshot")
    beets.set_album_ids_for_release(release_id, [7])
    beets.set_tracks_for_release(release_id, SNAPSHOT_TRACKS)
    return beets


def _release(release_id: str, *, suffix: str = "") -> BeetsWorldRelease:
    return BeetsWorldRelease(
        release_id=release_id,
        artist="Boundary Archivist",
        album=f"Exact pressing {suffix}".strip(),
        year=2001,
        track_count=2,
    )


def _request(
    request_id: int,
    release_id: str,
) -> dict[str, object]:
    return make_request_row(
        id=request_id,
        status="imported",
        mb_release_id=release_id,
        discogs_release_id=(
            release_id if release_id == DISCOGS_TARGET else None
        ),
    )


def _show(db: FakePipelineDB, request_id: int, world: BeetsWorld) -> str:
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        cmd_show(db, argparse.Namespace(
            id=request_id,
            beets_db=str(world.library_db),
            beets_directory=str(world.library_root),
        ))
    return stdout.getvalue()


class TestCurrentLibraryCliRealBeets(unittest.TestCase):
    def test_mb_moved_path_is_resolved_from_current_beets(self) -> None:
        with BeetsWorld(REPO) as world:
            initial = world.import_release(_release(MB_TARGET))
            moved = world.relocate_release_out_of_band(
                MB_TARGET,
                world.library_root / "Unicodé" / "曖昧 current",
                store_relative_paths=True,
            )
            self.assertNotEqual(initial.album_path, moved.album_path)
            db = FakePipelineDB()
            db.seed_request(_request(1, MB_TARGET))

            output = _show(db, 1, world)

            self.assertIn("Current Library: unique", output)
            self.assertIn(f"Current Path:    {moved.album_path}", output)
            self.assertNotIn(initial.album_path, output)

    def test_modern_and_legacy_discogs_render_the_same_unique_authority(self) -> None:
        for legacy in (False, True):
            with self.subTest(layout="legacy" if legacy else "modern"):
                with BeetsWorld(REPO) as world:
                    snapshot = world.import_release(_release(DISCOGS_TARGET))
                    world.set_discogs_identity_layout(
                        DISCOGS_TARGET, legacy=legacy,
                    )
                    world.set_release_paths_relative(DISCOGS_TARGET)
                    db = FakePipelineDB()
                    db.seed_request(_request(2, DISCOGS_TARGET))

                    output = _show(db, 2, world)

                    self.assertIn("Current Library: unique", output)
                    self.assertIn(
                        f"Current Path:    {snapshot.album_path}", output,
                    )
                    self.assertNotIn("/poisoned/request/cache", output)

    def test_same_metadata_sibling_is_missing_and_duplicates_are_ambiguous(
        self,
    ) -> None:
        with BeetsWorld(REPO) as world:
            world.import_release(_release(MB_SIBLING))
            db = FakePipelineDB()
            db.seed_request(_request(3, MB_TARGET))
            missing = _show(db, 3, world)
        self.assertIn("Current Library: missing", missing)
        self.assertNotIn("/poisoned/request/cache", missing)

        with BeetsWorld(REPO) as world:
            world.import_release(_release(MB_TARGET, suffix="one"))
            world.import_duplicate_release(_release(MB_TARGET, suffix="two"))
            db = FakePipelineDB()
            db.seed_request(_request(4, MB_TARGET))
            ambiguous = _show(db, 4, world)
        self.assertIn("Current Library: ambiguous", ambiguous)
        self.assertIn("Reason:          multiple_matches", ambiguous)
        self.assertIn("Album IDs:", ambiguous)
        self.assertNotIn("/poisoned/request/cache", ambiguous)

    def test_open_and_resolver_failures_render_typed_unavailable(self) -> None:
        db = FakePipelineDB()
        db.seed_request(_request(5, MB_TARGET))
        args = argparse.Namespace(
            id=5, beets_db=None, beets_directory=None,
        )

        for label, open_beets_fn in (
            ("open", _fail_open_beets),
            ("resolver", lambda **_kwargs: _FailingResolverBeets()),
        ):
            with self.subTest(failure=label):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    cmd_show(
                        db,
                        args,
                        open_beets_fn=open_beets_fn,
                    )
                output = stdout.getvalue()
                self.assertIn(
                    "Current Library: unavailable (manual review)", output,
                )
                self.assertIn("Reason:          beets_unavailable", output)

    def test_snapshot_resolver_runs_once_for_every_request_layout(self) -> None:
        layouts = (
            ("musicbrainz", MB_TARGET, None),
            ("modern_discogs", DISCOGS_TARGET, DISCOGS_TARGET),
            ("legacy_discogs", DISCOGS_TARGET, None),
        )
        for index, (label, release_id, discogs_id) in enumerate(layouts, 30):
            with self.subTest(layout=label):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=index,
                    status="imported",
                    mb_release_id=release_id,
                    discogs_release_id=discogs_id,
                ))
                beets = _snapshot_beets(release_id)
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    cmd_show(
                        db,
                        argparse.Namespace(
                            id=index,
                            beets_db=None,
                            beets_directory=None,
                        ),
                        open_beets_fn=lambda **_kwargs: beets,
                    )

                self.assertIn(
                    "Current Path:    /snapshot/current", stdout.getvalue(),
                )
                self.assertEqual(len(beets.resolve_current_release_calls), 1)
                self.assertEqual(
                    beets.resolve_current_release_calls[0].release_id,
                    release_id,
                )
                self.assertEqual(
                    beets.get_tracks_by_mb_release_id_calls, [],
                    "CLI must not reread tracks outside the resolver snapshot",
                )


class TestCurrentLibraryApiRealBeets(_FakeDbWebServerCase):
    def _get_detail(self, world: BeetsWorld, request_id: int) -> tuple[int, dict]:
        import web.server as srv

        prior = (srv._beets, srv.beets_db_path, srv.beets_library_root)
        srv._beets = None
        srv.beets_db_path = str(world.library_db)
        srv.beets_library_root = str(world.library_root)
        try:
            return self._get(f"/api/pipeline/{request_id}")
        finally:
            srv._beets, srv.beets_db_path, srv.beets_library_root = prior

    def test_api_returns_fresh_path_and_tracks_without_request_cache(self) -> None:
        with BeetsWorld(REPO) as world:
            initial = world.import_release(_release(MB_TARGET))
            moved = world.relocate_release_out_of_band(
                MB_TARGET,
                world.library_root / "API" / "moved-current",
                store_relative_paths=True,
            )
            self.db.seed_request(_request(10, MB_TARGET))

            status, data = self._get_detail(world, 10)

        self.assertEqual(status, 200)
        self.assertNotIn("imported_path", data["request"])
        self.assertEqual(data["current_library"]["state"], "unique")
        self.assertEqual(data["current_library"]["path"], moved.album_path)
        self.assertEqual(len(data["beets_tracks"]), 2)

    def test_api_modern_and_legacy_discogs_share_unique_authority(self) -> None:
        for legacy in (False, True):
            with self.subTest(layout="legacy" if legacy else "modern"):
                with BeetsWorld(REPO) as world:
                    snapshot = world.import_release(_release(DISCOGS_TARGET))
                    world.set_discogs_identity_layout(
                        DISCOGS_TARGET, legacy=legacy,
                    )
                    world.set_release_paths_relative(DISCOGS_TARGET)
                    self.db.seed_request(_request(20, DISCOGS_TARGET))

                    status, data = self._get_detail(world, 20)

                self.assertEqual(status, 200)
                self.assertEqual(data["current_library"]["state"], "unique")
                self.assertEqual(
                    data["current_library"]["release_source"], "discogs",
                )
                self.assertEqual(
                    data["current_library"]["path"], snapshot.album_path,
                )
                self.assertNotIn("imported_path", data["request"])

    def test_api_preserves_real_missing_and_ambiguous_states(self) -> None:
        with BeetsWorld(REPO) as world:
            world.import_release(_release(MB_SIBLING))
            self.db.seed_request(_request(11, MB_TARGET))
            status, missing = self._get_detail(world, 11)
        self.assertEqual(status, 200)
        self.assertEqual(missing["current_library"]["state"], "missing")
        self.assertNotIn("beets_tracks", missing)

        with BeetsWorld(REPO) as world:
            world.import_release(_release(MB_TARGET, suffix="one"))
            world.import_duplicate_release(_release(MB_TARGET, suffix="two"))
            self.db.seed_request(_request(11, MB_TARGET))
            status, ambiguous = self._get_detail(world, 11)
        self.assertEqual(status, 200)
        self.assertEqual(ambiguous["current_library"]["state"], "ambiguous")
        self.assertEqual(
            ambiguous["current_library"]["reason"], "multiple_matches",
        )
        self.assertEqual(len(ambiguous["current_library"]["album_ids"]), 2)
        self.assertNotIn("beets_tracks", ambiguous)

    def test_api_open_and_resolver_failures_are_typed_unavailable(self) -> None:
        import web.server as srv

        self.db.seed_request(_request(40, MB_TARGET))
        prior = (srv._beets, srv.beets_db_path, srv.beets_library_root)
        with TemporaryDirectory() as invalid_database_path:
            srv._beets = None
            srv.beets_db_path = invalid_database_path
            srv.beets_library_root = invalid_database_path
            try:
                status, open_failure = self._get("/api/pipeline/40")
            finally:
                srv._beets, srv.beets_db_path, srv.beets_library_root = prior

        self.assertEqual(status, 200)
        self.assertEqual(open_failure["current_library"], {
            "state": "unavailable",
            "reason": "beets_unavailable",
            "manual_review": True,
        })
        self.assertNotIn("beets_tracks", open_failure)

        prior_beets = srv._beets
        failing_beets = _FailingResolverBeets()
        srv._beets = failing_beets
        try:
            status, read_failure = self._get("/api/pipeline/40")
        finally:
            srv._beets = prior_beets

        self.assertEqual(status, 200)
        self.assertEqual(read_failure["current_library"], {
            "state": "unavailable",
            "reason": "beets_unavailable",
            "manual_review": True,
        })
        self.assertNotIn("beets_tracks", read_failure)
        self.assertEqual(len(failing_beets.resolve_current_release_calls), 1)

    def test_api_projects_one_snapshot_for_every_request_layout(self) -> None:
        import web.server as srv

        layouts = (
            ("musicbrainz", MB_TARGET, None),
            ("modern_discogs", DISCOGS_TARGET, DISCOGS_TARGET),
            ("legacy_discogs", DISCOGS_TARGET, None),
        )
        for label, release_id, discogs_id in layouts:
            with self.subTest(layout=label):
                self.db.seed_request(make_request_row(
                    id=50,
                    status="imported",
                    mb_release_id=release_id,
                    discogs_release_id=discogs_id,
                ))
                beets = _snapshot_beets(release_id)
                prior_beets = srv._beets
                srv._beets = beets
                try:
                    status, data = self._get("/api/pipeline/50")
                finally:
                    srv._beets = prior_beets

                self.assertEqual(status, 200)
                self.assertEqual(data["beets_tracks"], EXPECTED_SNAPSHOT_TRACKS)
                self.assertEqual(len(beets.resolve_current_release_calls), 1)
                self.assertEqual(
                    beets.resolve_current_release_calls[0].release_id,
                    release_id,
                )
                self.assertEqual(
                    beets.get_tracks_by_mb_release_id_calls, [],
                    "API must project tracks from the resolver snapshot",
                )


if __name__ == "__main__":
    unittest.main()
